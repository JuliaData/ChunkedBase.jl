# When splitting the work among multiple tasks, we aim for each task to have at least this
# many bytes of input to work on (even if it means that some tasks will have nothing to process)
# This is to avoid overhead from too many task switches.
# TODO: make this configurable and find a good default (the current 16 KiB is a guess)
const MIN_TASK_SIZE_IN_BYTES = 16 * 1024

_comment_to_bytes(x::AbstractString) = Vector{UInt8}(x)
_comment_to_bytes(x::Char) = _comment_to_bytes(ncodeunits(x) > 1 ? string(x) : UInt8(x))
_comment_to_bytes(x::UInt8) = [x]
_comment_to_bytes(x::Vector{UInt8}) = x
_comment_to_bytes(::Nothing) = nothing

"""
    ChunkingContext(
        buffersize::Integer,
        nworkers::Integer,
        limit::Integer,
        comment::Union{Nothing,UInt8,String,Char,Vector{UInt8}}
    ) -> ChunkingContext

A context object used to coordinate parallel parsing of a single file, chunk by chunk.

The user can use this object to specify the size of the byte buffer(s) to allocate, the
number of worker tasks to spawn and the maximum number of rows to parse in `parse_file_parallel`.

# Arguments:
- `buffersize`: the size of the byte buffer to allocate .
    If the input is bigger than `buffersize`, a secondary `ChunkingContext` object will be used to
    double-buffer the input, which will allocate a new buffer of the same size as `buffersize`.
- `nworkers`: the number of worker tasks that should be spawned in `parse_file_parallel`
- `limit`: the maximum number of rows to parse, see `limit_eols!`
- `comment`: the comment prefix to skip, if any

# Notes:
- One can use the `id` and `buffer_refills` fields to uniquely identify a chunk of input.
The `id` field is necessary because we internally create a secondary `ChunkingContext` object, with
`id` equal to the `id` of the original `ChunkingContext` + 1.
- The `counter` field is used to synchronize the parser/consumer tasks.
- The `newline_positions` field is used to store the newline positions in the input.
- The `bytes` field is used to store the raw bytes ingested from the input.
- `comment` can be used to skip the *initial* comment lines in the `skip_rows_init!`. This value is also passed to `populate_result_buffer!` for user to apply handle commented rows in the middle of the file during parsing (`_startswith` could be used to do the check).
- The `buffersize` should be large enough to fit the longest row in the input, otherwise the lexer will fail.
- The `buffersize` should be chosen such that each of the `nworkers` tasks has enough bytes to work on. Using 1MiB per task seems to work reasonably well in practice.

# See also:
- [`parse_file_parallel`](@ref), [`parse_file_serial`](@ref)
"""
struct ChunkingContext
    id::Int                                  # id of the chunking context (1 or 2)
    counter::TaskCounter                     # synchronization mechanism to coordinate parsing
    newline_positions::BufferedVector{Int32} # positions of newlines in the bytes
    bytes::Vector{UInt8}                     # raw bytes ingested from the input
    nworkers::Int                            # number of worker tasks
    limit::Int                               # maximum number of rows to parse, see `limit_eols!`
    # byte prefix to skip, used in `skip_rows_init!` and handed to `populate_result_buffer!`
    # for user to handle with consistently (`_startswith` could be used to do the check)
    comment::Union{Nothing,Vector{UInt8}}
    # number of times we refilled the buffer, can be combined with `id` to uniquely identify a chunk
    buffer_refills::Base.RefValue{Int}
end
function ChunkingContext(buffersize::Integer, nworkers::Integer, limit::Integer, comment::Union{Nothing,UInt8,String,Char,Vector{UInt8}})
    (4 <= buffersize <= typemax(Int32)) || throw(ArgumentError("`buffersize` argument must be larger than 4 and smaller than 2_147_483_648 bytes."))
    (0 < nworkers < 256) || throw(ArgumentError("`nworkers` argument must be larger than 0 and smaller than 256."))
    (0 <= limit <= typemax(Int)) || throw(ArgumentError("`limit` argument must be positive and smaller than 9_223_372_036_854_775_808."))
    # TRACING #  clear_traces!(nworkers)
    return ChunkingContext(
        1,
        TaskCounter(),
        BufferedVector{Int32}(Int32[0], 1),
        Vector{UInt8}(undef, buffersize),
        nworkers,
        limit,
        _comment_to_bytes(comment),
        Ref(0),
    )
end
# Convenience for double-buffering
function ChunkingContext(ctx::ChunkingContext)
    out = ChunkingContext(
        ctx.id + 1,
        TaskCounter(),
        BufferedVector{Int32}(Vector{Int32}(undef, max(1, length(ctx.newline_positions))), 1),
        similar(ctx.bytes),
        ctx.nworkers,
        ctx.limit,
        ctx.comment,
        Ref(0),
    )
    out.newline_positions.elements[1] = 0
    return out
end
tasks_per_chunk(ctx::ChunkingContext) = ctx.nworkers
total_result_buffers_count(ctx::ChunkingContext) = 2tasks_per_chunk(ctx)
last_newline_at(ctx::ChunkingContext) = Int(last(ctx.newline_positions))
function should_use_parallel(ctx::ChunkingContext, _force)
    return !(
        _force === :serial ||
        ((_force !== :parallel) && (Threads.nthreads() == 1 || ctx.nworkers == 1 || last_newline_at(ctx) < MIN_TASK_SIZE_IN_BYTES))
    )
end

# Instead of splitting the newlines among `nworker` tasks equally, we try to ensure that each task
# has at least `MIN_TASK_SIZE_IN_BYTES` bytes of input to work on. For smaller inputs or for
# the last, trialing bytes of a bigger file, there won't be enough newlines to utilize each
# of the `nworkers` tasks properly, so we'll send out fewer chunks of work that are bigger to
# the parsing queue.
function estimate_task_size(ctx::ChunkingContext)
    eols = ctx.newline_positions
    length(eols) == 1 && return 1 # empty file
    bytes_to_parse = last(eols)
    rows = length(eols) # actually rows + 1
    buffersize = length(ctx.bytes)
    # There are `2*nworkers` result buffers total, but there are `nworkers` tasks per chunk
    prorated_maxtasks = ceil(Int, tasks_per_chunk(ctx) * (bytes_to_parse / buffersize))
    # Lower bound is 2 because length(eols) == 2 => 1 row
    # bump min rows if average row is much smaller than MIN_TASK_SIZE_IN_BYTES
    min_rows = max(2, cld(MIN_TASK_SIZE_IN_BYTES, cld(bytes_to_parse, rows)))
    return min(max(min_rows, cld(rows, prorated_maxtasks)), rows)
end
