function parse_file_serial(
    lexer::Lexer,
    parsing_ctx::AbstractParsingContext,
    consume_ctx::AbstractConsumeContext,
    chunking_ctx::ChunkingContext,
)
    row_num = 1
    try
        @inbounds while true
            limit_eols!(chunking_ctx, row_num) && break
            task_size = estimate_task_size(chunking_ctx)
            task_start = Int32(1)
            for task in Iterators.partition(eachindex(chunking_ctx.newline_positions), task_size)
                setup_tasks!(consume_ctx, chunking_ctx, 1)
                task_end = Int32(last(task))
                newline_segment = @view(chunking_ctx.newline_positions.elements[task_start:task_end])
                process!(parsing_ctx, newline_segment, chunking_ctx.bytes, 0)
                consume!(consume_ctx, ParsedPayload(row_num, length(task) - 1, parsing_ctx, task_start))
                row_num += Int(task_end - task_start)
                task_start = task_end
                task_done!(consume_ctx, chunking_ctx)
                sync_tasks(chunking_ctx)
            end
            lexer.done && break
            read_and_lex!(lexer, chunking_ctx)
        end # while true
    catch e
        close(chunking_ctx.counter, e)
        cleanup(consume_ctx, e)
        rethrow(e)
    end
    return nothing
end
