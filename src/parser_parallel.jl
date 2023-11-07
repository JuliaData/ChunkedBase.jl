# What the `read_and_lex_task!` task submits to the parser tasks via a Channel
# * task_start: first index in the newline_positions buffer the parser task should process
# * task_end: first index in the newline_positions buffer the parser task should process
# * row_num: the (global) row number of the first newline in the chunk
# * task_num: the index of the task (1-based)
# * use_current_context: whether to use the current or the next chunking context (double-buffering)
const SubtaskMetadata = @NamedTuple{task_start::Int32, task_end::Int32, row_num::Int, task_num::Int, use_current_context::Bool}

function submit_lexed_rows!(parsing_queue, consume_ctx, chunking_ctx, row_num)
    task_size = estimate_task_size(chunking_ctx)
    ntasks = cld(length(chunking_ctx.newline_positions), task_size)
    # Set the expected number of parsing tasks
    # TRACING # chunking_ctx.id == 1 ? push!(ChunkedBase.T1, time_ns()) : push!(ChunkedBase.T2, time_ns())
    setup_tasks!(consume_ctx, chunking_ctx, ntasks)
    # Send task definitions (segment of `eols` to process) to the queue
    task_start = Int32(1)
    task_num = 1
    for task in Iterators.partition(eachindex(chunking_ctx.newline_positions), task_size)
        task_end = Int32(last(task))
        put!(parsing_queue, SubtaskMetadata((task_start, task_end, row_num, task_num, chunking_ctx.id == 1)))
        row_num += Int(task_end - task_start)
        task_start = task_end
        task_num += 1
    end
    return row_num
end

function read_and_lex_task!(
    parsing_queue::Channel{SubtaskMetadata}, # To submit work for parser tasks (which segment of newlines to process)
    lexer::Lexer,                            # Finding newlines
    chunking_ctx::ChunkingContext,           # Holds raw bytes, synchronization objects and stores newline positions
    chunking_ctx_next::ChunkingContext,      # double-buffering
    consume_ctx::AbstractConsumeContext      # user-provided data (to overload setup_tasks!)
)
    limit_eols!(chunking_ctx, 1) && return
    row_num = submit_lexed_rows!(parsing_queue, consume_ctx, chunking_ctx, 1)
    @inbounds while true
        # Start parsing _next_ chunk of input
        if !lexer.done
            _last_newline_at = last_newline_at(chunking_ctx)
            # We need to copy the trailing bytes from the previous buffer to the next one
            # so when we shift and refill in `read_and_lex!`, we can continue where we left off.
            unsafe_copyto!(chunking_ctx_next.bytes, _last_newline_at, chunking_ctx.bytes, _last_newline_at, length(chunking_ctx.bytes) - _last_newline_at + 1)
            read_and_lex!(lexer, chunking_ctx_next, _last_newline_at)
            limit_eols!(chunking_ctx_next, row_num) && break
            row_num = submit_lexed_rows!(parsing_queue, consume_ctx, chunking_ctx_next, row_num)
        end
        # Wait for parsers to finish processing current chunk
        sync_tasks(chunking_ctx)
        lexer.done && break
        # Switch contexts
        chunking_ctx, chunking_ctx_next = chunking_ctx_next, chunking_ctx
    end
    sync_tasks(chunking_ctx_next)
end

function process_and_consume_task(
    worker_id::Int,                                 # unique identifier of this task
    parsing_queue::Channel{SubtaskMetadata},        # where workers receive work
    result_buffers::Vector{<:AbstractResultBuffer}, # where we store parsed results
    consume_ctx::AbstractConsumeContext,            # user-provided data (what to to with the parsed results)
    parsing_ctx::AbstractParsingContext,            # library-provided data (to distinguish JSONL and CSV processing)
    chunking_ctx::ChunkingContext,                  # internal data to facilitate chunking and synchronization
    chunking_ctx_next::ChunkingContext,             # double-buffering
    ::Type{CT}                                      # compile time known data for parser
) where {CT}
    # TRACING # trace = get_parser_task_trace(worker_id)
    _comment = chunking_ctx.comment
    try
        @inbounds while true
            (task_start, task_end, row_num, task_num, use_current_context) = take!(parsing_queue)
            iszero(task_end) && break # zero is a signal to stop
            # We prepared 2 * nworkers result buffers, as there are might 2 chunks in flight and
            # since the user might provide their own consume! methods which won't block like the default
            # consume!, not separating the result buffers per chunk could lead to data corruption if
            # the results from the 2nd chunk are ready before the 1st chunk is consumed.
            result_buf = result_buffers[task_num + (use_current_context ? 0 : tasks_per_chunk(chunking_ctx))]
            # TRACING # push!(trace, time_ns())
            ctx = ifelse(use_current_context, chunking_ctx, chunking_ctx_next)
            # Defined by the library using ChunkedBase via overload on the specific AbstractResultBuffer and AbstractParsingContext
            newline_segment = @view(ctx.newline_positions.elements[task_start:task_end])
            populate_result_buffer!(result_buf, newline_segment, parsing_ctx, ctx.bytes, _comment, CT)
            # Defined by the user via overload on consume_ctx
            consume!(consume_ctx, ParsedPayload(row_num, Int(task_end - task_start), result_buf, parsing_ctx, ctx, task_start))
            task_done!(consume_ctx, ctx)
            # TRACING # push!(trace, time_ns())
        end
    catch e
        ce = CapturedException(e, catch_backtrace())
        # If there was an exception, immediately stop processing the queue
        isopen(parsing_queue) && close(parsing_queue, ce)
        # if the io_task was waiting for work to finish, we'll interrupt it here
        isopen(chunking_ctx.counter) && close(chunking_ctx.counter, ce)
        isopen(chunking_ctx_next.counter) && close(chunking_ctx_next.counter, ce)
    end
end

"""
    parse_file_parallel(
        lexer::NewlineLexers.Lexer,
        parsing_ctx::AbstractParsingContext,
        consume_ctx::AbstractConsumeContext,
        chunking_ctx::ChunkingContext,
        result_buffers::Vector{<:AbstractResultBuffer},
        ::Type{CT}=Tuple{}
    ) where {CT} -> Nothing

    Parse the file in `lexer.io` in parallel using `chunking_ctx.nworkers` tasks. User must provide
    a `populate_result_buffer!` method which is used to parse ingested data in `chunking_ctx.bytes`, using the
    newline positions in `chunking_ctx.newline_positions` as row boundaries into the `result_buffers`.
    The `consume!` method is called after each `populate_result_buffer!` call, so the user can process
    the parsed results in parallel. No `result_buffer` is accessed by more than one task at a time.

    # Arguments:
    * `lexer`: a `NewlineLexers.Lexer` object which is used to find newline positions in the input. The
    type of the lexer affects whether the search is quote-aware or not.
    * `parsing_ctx`: a user-provided object which is passed to `populate_result_buffer!`
    * `consume_ctx`: a user-provided object which is passed to `consume!`
    * `chunking_ctx`: an internal object that is used to keep track of the current chunk of data being processed
    * `result_buffers`: a vector of user-provided objects which are used to store the parsed results
    * `CT`: an optional, compile-time known type which is passed to `populate_result_buffer!`

    # Exceptions:
    * `UnmatchedQuoteError`: if the input ends with an unmatched quote
    * `NoValidRowsInBufferError`: if not a single newline was found in the input buffer
    * `CapturedException`: if an exception was thrown in one of the parser/consumer tasks

    # Notes:
    * The `chunking_ctx` is assumed to be filled with data whose newline positions are already detected, e.g.
    by calling `read_and_lex!` with the `lexer` object on it.
    * If the input is bigger than `chunking_ctx.bytes`, a secondary `chunking_ctx` object will be used to
    double-buffer the input, which will allocate a new buffer of the same size as `chunking_ctx.bytes`.
    * This function spawns `chunking_ctx.nworkers` + 1 tasks.

    See also [`populate_result_buffer!`](@ref), [`consume!`](@ref), [`parse_file_serial`](@ref).
"""
function parse_file_parallel(
    lexer::Lexer,
    parsing_ctx::AbstractParsingContext,
    consume_ctx::AbstractConsumeContext,
    chunking_ctx::ChunkingContext,
    result_buffers::Vector{<:AbstractResultBuffer},
    ::Type{CT}=Tuple{}
) where {CT}
    @assert chunking_ctx.id == 1
    length(result_buffers) != total_result_buffers_count(chunking_ctx) && ArgumentError("Expected $(total_result_buffers_count(chunking_ctx)) result buffers, got $(length(result_buffers)).")

    parsing_queue = Channel{SubtaskMetadata}(Inf)
    if lexer.done
        chunking_ctx_next = chunking_ctx
    else
        chunking_ctx_next = ChunkingContext(chunking_ctx)
    end
    parser_tasks = sizehint!(Task[], chunking_ctx.nworkers)
    for i in 1:chunking_ctx.nworkers
        t = Threads.@spawn process_and_consume_task($i, $parsing_queue, $result_buffers, $consume_ctx, $parsing_ctx, $chunking_ctx, $chunking_ctx_next, $CT)
        push!(parser_tasks, t)
    end

    try
        io_task = Threads.@spawn read_and_lex_task!($parsing_queue, $lexer, $chunking_ctx, $chunking_ctx_next, $consume_ctx)
        wait(io_task)
    catch e
        isopen(parsing_queue) && close(parsing_queue, e)
        cleanup(consume_ctx, e)
        rethrow()
    end
    # Cleanup
    for _ in 1:chunking_ctx.nworkers
        put!(parsing_queue, SubtaskMetadata((Int32(0), Int32(0), 0, 0, true)))
    end
    foreach(wait, parser_tasks)
    close(parsing_queue)
    return nothing
end
