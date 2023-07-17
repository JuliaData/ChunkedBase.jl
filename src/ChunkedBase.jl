module ChunkedBase

using Mmap
using CodecZlibNG

using NewlineLexers
using SentinelArrays.BufferedVectors

const MIN_TASK_SIZE_IN_BYTES = 16 * 1024

# populate_result_buffer!(result_buf::AbstractResultBuffer, newlines::AbstractVector{Int32}, parsing_ctx::AbstractParsingContext, comment::Union{Nothing,Vector{UInt8}}=nothing, ::Type{CT}=Tuple{}) where {CT}
function populate_result_buffer! end

abstract type AbstractParsingContext end

# Synchronization mechanism -- after we lexed all rows, we split them in N tasks and TaskCounter
# in ChunkingContext will block the io/lexer to overwrite the current chunk unless workers
# report back N times that they are done with their tasks.
include("TaskCounters.jl")
using .TaskCounters

_comment_to_bytes(x::AbstractString) = Vector{UInt8}(x)
_comment_to_bytes(x::Char) = _comment_to_bytes(ncodeunits(x) > 1 ? string(x) : UInt8(x))
_comment_to_bytes(x::UInt8) = [x]
_comment_to_bytes(x::Vector{UInt8}) = x
_comment_to_bytes(::Nothing) = nothing
struct ChunkingContext
    id::Int
    counter::TaskCounter
    newline_positions::BufferedVector{Int32}
    bytes::Vector{UInt8}
    nworkers::Int
    limit::Int
    comment::Union{Nothing,Vector{UInt8}}
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
    )
end
function ChunkingContext(ctx::ChunkingContext)
    out = ChunkingContext(
        ctx.id + 1,
        TaskCounter(),
        BufferedVector{Int32}(Vector{Int32}(undef, max(1, length(ctx.newline_positions))), 1),
        similar(ctx.bytes),
        ctx.nworkers,
        ctx.limit,
        ctx.comment,
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

# We split the detected newlines equally among thr nworkers parsing tasks, but each
# unit of work should contain at least 16 KiB of raw bytes (MIN_TASK_SIZE_IN_BYTES).
function estimate_task_size(ctx::ChunkingContext)
    eols = ctx.newline_positions
    length(eols) == 1 && return 1 # empty file
    bytes_to_parse = last(eols)
    rows = length(eols) # actually rows + 1
    buffersize = length(ctx.bytes)
    # There are 2*nworkers result buffers total, but there are nworkers tasks per chunk
    prorated_maxtasks = ceil(Int, tasks_per_chunk(ctx) * (bytes_to_parse / buffersize))
    # Lower bound is 2 because length(eols) == 2 => 1 row
    # bump min rows if average row is much smaller than MIN_TASK_SIZE_IN_BYTES
    min_rows = max(2, cld(MIN_TASK_SIZE_IN_BYTES, cld(bytes_to_parse, rows)))
    return min(max(min_rows, cld(rows, prorated_maxtasks)), rows)
end

abstract type AbstractResultBuffer end

include("payload.jl")

include("exceptions.jl")
include("read_and_lex_utils.jl")
include("read_and_lex.jl")
include("ConsumeContexts.jl")
using .ConsumeContexts

include("parser_serial.jl")
include("parser_parallel.jl")

export ChunkingContext, tasks_per_chunk, total_result_buffers_count
export AbstractParsingContext
export AbstractConsumeContext, setup_tasks!, consume!, task_done!, cleanup
export AbstractResultBuffer
export TaskCounter
export ParsedPayload, PayloadOrderer
export SkipContext
export Lexer
export parse_file_serial, parse_file_parallel, populate_result_buffer!

# TRACING # const PARSER_TASKS_TIMES = [UInt[]]
# TRACING # const IO_TASK_TIMES = UInt[]
# TRACING # const LEXER_TASK_TIMES = UInt[]
# TRACING # const T1 = UInt[]
# TRACING # const T2 = UInt[]
# TRACING # get_parser_task_trace(i) = PARSER_TASKS_TIMES[i]
# TRACING # function clear_traces!(nworkers::Int=Threads.nthreads())
# TRACING #     for _ in (length(PARSER_TASKS_TIMES)+1:nworkers)
# TRACING #         push!(PARSER_TASKS_TIMES, UInt[])
# TRACING #     end
# TRACING #     empty!(IO_TASK_TIMES)
# TRACING #     empty!(LEXER_TASK_TIMES)
# TRACING #     empty!(T1)
# TRACING #     empty!(T2)
# TRACING #     foreach(empty!, PARSER_TASKS_TIMES)
# TRACING #     return nothing
# TRACING # end
# TRACING # function dump_traces(path)
# TRACING #     open(path, "w") do io
# TRACING #         write(io, UInt32(length(IO_TASK_TIMES)), IO_TASK_TIMES)
# TRACING #         write(io, UInt32(length(LEXER_TASK_TIMES)), LEXER_TASK_TIMES)
# TRACING #         write(io, UInt32(length(T1)), T1)
# TRACING #         write(io, UInt32(length(T2)), T2)
# TRACING #
# TRACING #         write(io, UInt32(length(PARSER_TASKS_TIMES)))
# TRACING #         for x in PARSER_TASKS_TIMES
# TRACING #             write(io, UInt32(length(x)), x)
# TRACING #         end
# TRACING #     end
# TRACING #     return nothing
# TRACING # end
# TRACING # function load_traces!(path)
# TRACING #     open(path, "r") do io
# TRACING #         read!(io, resize!(IO_TASK_TIMES, read(io, UInt32)))
# TRACING #         read!(io, resize!(LEXER_TASK_TIMES, read(io, UInt32)))
# TRACING #         read!(io, resize!(T1, read(io, UInt32)))
# TRACING #         read!(io, resize!(T2, read(io, UInt32)))
# TRACING #
# TRACING #         resize!(PARSER_TASKS_TIMES, read(io, UInt32))
# TRACING #         for x in PARSER_TASKS_TIMES
# TRACING #             read!(io, resize!(x, read(io, UInt32)))
# TRACING #         end
# TRACING #     end
# TRACING #     return nothing
# TRACING # end

end
