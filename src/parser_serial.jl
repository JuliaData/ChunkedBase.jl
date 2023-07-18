function parse_file_serial(
    lexer::Lexer,
    parsing_ctx::AbstractParsingContext,
    consume_ctx::AbstractConsumeContext,
    chunking_ctx::ChunkingContext,
    result_buf::AbstractResultBuffer,
    ::Type{CT}=Tuple{},
) where {CT}
    row_num = 1
    _comment = chunking_ctx.comment
    try
        @inbounds while true
            limit_eols!(chunking_ctx, row_num) && break
            task_size = estimate_task_size(chunking_ctx)
            task_start = Int32(1)
            for task in Iterators.partition(eachindex(chunking_ctx.newline_positions), task_size)
                # TRACING # chunking_ctx.id == 1 ? push!(ChunkedBase.T1, time_ns()) : push!(ChunkedBase.T2, time_ns())
                setup_tasks!(consume_ctx, chunking_ctx, 1)
                task_end = Int32(last(task))
                newline_segment = @view(chunking_ctx.newline_positions.elements[task_start:task_end])
                populate_result_buffer!(result_buf, newline_segment, parsing_ctx, chunking_ctx.bytes, _comment, CT)
                consume!(consume_ctx, ParsedPayload(row_num, length(task) - 1, result_buf, parsing_ctx, chunking_ctx, task_start))
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
