module ConsumeContexts

using ..ChunkedBase: ChunkedBase, ChunkingContext, MIN_TASK_SIZE_IN_BYTES
using ..ChunkedBase: AbstractResultBuffer, ParsedPayload, AbstractParsingContext, BufferedVector
using ..ChunkedBase: set!, dec!

export AbstractConsumeContext, SkipContext
export setup_tasks!, consume!, task_done!, sync_tasks, cleanup

# End users should subtype this to create custom consume contexts which are then
# used in `parse_file_parallel` and `parse_file_serial`, to dispatch on their
# `populate_result_buffer!` method.
abstract type AbstractConsumeContext end

"""
    consume!(consume_ctx::AbstractConsumeContext, payload::ParsedPayload{<:AbstractResultBuffer, <:AbstractParsingContext})

Override with your `AbstractConsumeContext` to provide a custom logic for processing the parsed results in `AbstractResultBuffer`.
The method is called from multiple tasks in parallel, just after each corresponding `task_buf` has been populated.
`task_buf` is only filled once per chunk and is only accessed by one task at a time.

See also [`consume!`](@ref), [`setup_tasks!`](@ref), [`setup_tasks!`](@ref), [`cleanup`](@ref), [`AbstractResultBuffer`](@ref)
"""
function consume! end
function setup_tasks! end
function task_done! end
function sync_tasks end
function cleanup end

"""
    setup_tasks!(consume_ctx::AbstractConsumeContext, chunking_ctx::ChunkingContext, ntasks::Int)

Set the number of units of work that the parser/consume tasks need to finish
(and report back as done via e.g. `task_done!`) before the current chunk of input data
is considered to be entirely processed.

This function is called just after the we're done detecting newline positions in the current
chunk of data and we are about to submit partitions of the detected newlines to the parse/consume tasks.

`ntasks` is between 1 and `nworkers` argument to `parse_file`, depending on the size of the input.
Most of the time, the value is `nworkers` is used, but for smaller buffer sizes, smaller files or
when handling the last bytes of the file, `ntasks` will be smaller as we try to ensure the minimal
average tasks size if terms of bytes of input is at least $(Base.format_bytes(MIN_TASK_SIZE_IN_BYTES)).
For `:serial` parsing mode, `ntasks` is always 1.

You should override this method when you further subdivide the amount of concurrent work on the chunk,
e.g. when you want to process each column separately in `@spawn` tasks, in which case you'd expect
there to be `ntasks * (1 + length(parsing_ctx.schema))` units of work per chunk
(in which case you'd have to manually call `task_done!` in the column-processing tasks).

See also [`consume!`](@ref), [`setup_tasks!`](@ref), [`task_done!`](@ref), [`cleanup`](@ref)
"""
function setup_tasks!(::AbstractConsumeContext, chunking_ctx::ChunkingContext, ntasks::Int)
    set!(chunking_ctx.counter, ntasks)
    return nothing
end

"""
    task_done!(consume_ctx::AbstractConsumeContext, chunking_ctx::ChunkingContext)

Decrement the expected number of remaining work units by one. Called after each `consume!` call.

The this function should be called `ntasks` times where `ntasks` comes from the corresponding
`setup_tasks!` call.

See also [`consume!`](@ref), [`setup_tasks!`](@ref), [`cleanup`](@ref)
"""
function task_done!(::AbstractConsumeContext, chunking_ctx::ChunkingContext)
    dec!(chunking_ctx.counter, 1)
    return nothing
end

# TODO: If we want to support schema inference, this would be a good place to sync a `Vector{TaskResultBuffer}` belonging to current `parsing_ctx`
"""
    sync_tasks(consume_ctx::AbstractConsumeContext, chunking_ctx::ChunkingContext)

Wait for all parse/consume tasks to report all expected units of work to be done.
Called after all work for the current chunk has been submitted to the parser/consume tasks
and we're about to refill it.

See also [`consume!`](@ref), [`setup_tasks!`](@ref), [`task_done!`](@ref), [`cleanup`](@ref)
"""
function sync_tasks(chunking_ctx::ChunkingContext)
    wait(chunking_ctx.counter)
    # TRACING # chunking_ctx.id == 1 ? push!(ChunkedBase.T1, time_ns()) : push!(ChunkedBase.T2, time_ns())
    return nothing
end
"""
    cleanup(consume_ctx::AbstractConsumeContext, e::Exception)

You can override this method do custom exception handling with your consume context.
This method is called immediately before a `rethrow()` which forwards the `e` further.

See also [`consume!`](@ref), [`setup_tasks!`](@ref), [`task_done!`](@ref), [`sync_tasks`](@ref)
"""
cleanup(::AbstractConsumeContext, ::Exception) = nothing

struct SkipContext <: AbstractConsumeContext end
consume!(::SkipContext, ::ParsedPayload) = nothing

end # module
