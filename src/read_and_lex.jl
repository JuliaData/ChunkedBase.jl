function readbytesall!(io::IOStream, buf, n::Integer)
    # TRACING # push!(IO_TASK_TIMES, time_ns())
    out = Base.readbytes!(io, buf, n; all = true)
    # TRACING # push!(IO_TASK_TIMES, time_ns())
    return out
end
function readbytesall!(io::IO, buf, n::Integer)
    # TRACING # push!(IO_TASK_TIMES, time_ns())
    out = Base.readbytes!(io, buf, n)
    # TRACING # push!(IO_TASK_TIMES, time_ns())
    return out
end

function prepare_buffer!(io::IO, buf::Vector{UInt8}, last_newline_at::Int)
    ptr = pointer(buf)
    buffersize = length(buf)
    @inbounds if last_newline_at == 0
        # This is the first time we saw the buffer, we'll fill it up and skip leading BOM
        bytes_read_in = readbytesall!(io, buf, buffersize)
        if bytes_read_in > 2 && _hasBOM(buf)
            bytes_read_in += prepare_buffer!(io, buf, 3)
            bytes_read_in -= 3
        end
    elseif last_newline_at < buffersize
        # We'll keep the bytes that are past the last newline, shifting them to the left
        # and refill the rest of the buffer.
        unsafe_copyto!(ptr, ptr + last_newline_at, buffersize - last_newline_at)
        bytes_read_in = @inbounds readbytesall!(io, @view(buf[buffersize - last_newline_at + 1:end]), last_newline_at)
    else
        # Last chunk was consumed entirely
        bytes_read_in = readbytesall!(io, buf, buffersize)
    end
    return bytes_read_in
end

function check_any_valid_rows(lexer, chunking_ctx)
    eols = chunking_ctx.newline_positions
    if (length(eols) == 0 || (length(eols) == 1 && first(eols) == 0)) && !eof(lexer.io) # TODO: check done instead of eof?
        close(lexer.io)
        throw(NoValidRowsInBufferError(length(chunking_ctx.bytes)))
    end
end

function handle_file_end!(lexer::Lexer, eols, end_pos)
    @inbounds if eof(lexer.io)
        # If the file ended with an unmatched quote, we throw an error
        !NewlineLexers.possibly_not_in_string(lexer) && (close(lexer.io); throw(UnmatchedQuoteError()))
        lexer.done = true
        # Insert a newline at the end of the file if there wasn't one
        # This is just to make `eols` contain both start and end `pos` of every single line
        last(eols) < end_pos && push!(eols, unsafe_trunc(Int32, end_pos) + Int32(1))
    end
end


function read_and_lex!(lexer::Lexer, chunking_ctx::ChunkingContext, _last_newline_at=last_newline_at(chunking_ctx))
    @assert !lexer.done

    empty!(chunking_ctx.newline_positions)
    push!(chunking_ctx.newline_positions, Int32(0))
    if eof(lexer.io) # Catches the empty input case
        lexer.done = true
        return nothing
    end

    bytes_read_in = prepare_buffer!(lexer.io, chunking_ctx.bytes, _last_newline_at)
    chunking_ctx.buffer_refills[] += 1
    # _last_newline_at == 0 && bytes_read_in == 0 && return nothing # only BOM / whitespace in the buffer

    start_pos = _last_newline_at == 0 ? 1 : length(chunking_ctx.bytes) - _last_newline_at + 1
    end_pos = start_pos + bytes_read_in - 1
    # TRACING #  push!(LEXER_TASK_TIMES, time_ns())
    find_newlines!(lexer, chunking_ctx.bytes, chunking_ctx.newline_positions, start_pos, end_pos)
    handle_file_end!(lexer, chunking_ctx.newline_positions, end_pos)
    check_any_valid_rows(lexer, chunking_ctx)
    # TRACING #  push!(LEXER_TASK_TIMES, time_ns())

    return nothing
end

function initial_read!(io, chunking_ctx, skip_leading_whitespace=false)
    # First ingestion of raw bytes from io
    buf = chunking_ctx.bytes
    buffersize = length(buf)

    # This is the first time we saw the buffer, we'll just fill it up
    bytes_read_in = readbytesall!(io, buf, buffersize)
    chunking_ctx.buffer_refills[] += 1

    # bytes_read_in = _skip_over_initial_whitespace_and_bom!(io, buf, bytes_read_in)
    starts_with_bom = bytes_read_in > 2 && _hasBOM(buf)

    if skip_leading_whitespace
        first_byte_to_check = (starts_with_bom ? 4 : 1)
        # First byte that is not a whitespace or a BOM, zero if the entire buffer is only white
        # space
        populated = view(buf, 1:bytes_read_in)
        first_valid_byte = something(findnext(_nonspace, populated, first_byte_to_check), 0)

        # We'll keep refilling the buffer until it we find a non-space or we run out of bytes
        while first_valid_byte == 0
            bytes_read_in = readbytesall!(io, buf, buffersize)
            bytes_read_in == 0 && return bytes_read_in
            populated = view(buf, 1:bytes_read_in)
            first_valid_byte = something(findnext(_nonspace, populated, 1), 0)
        end

        # We found a non-space byte -- we'll left-shift the spaces before it so that the buffer
        # begins with a valid value and we'll try to refill the rest of the buffer.
        # If first_valid_byte was already at the beginnig of the buffer, we don't have to do
        # anything.
        skip_over = first_valid_byte - 1
        if skip_over > 0
            bytes_read_in += prepare_buffer!(io, buf, skip_over)
            bytes_read_in -= skip_over
        end
    elseif starts_with_bom
        bytes_read_in += prepare_buffer!(io, buf, 3)
        bytes_read_in -= 3
    end

    return bytes_read_in
end

function initial_lex!(lexer, chunking_ctx, bytes_read_in)
    @assert !lexer.done
    @assert last_newline_at(chunking_ctx) == 0
    eof(lexer.io) && (lexer.done = true)

    # Find newlines if the input is not empty
    if bytes_read_in > 0
        # TRACING #  push!(LEXER_TASK_TIMES, time_ns())
        find_newlines!(lexer, chunking_ctx.bytes, chunking_ctx.newline_positions, 1, bytes_read_in)
        handle_file_end!(lexer, chunking_ctx.newline_positions, bytes_read_in)
        check_any_valid_rows(lexer, chunking_ctx)
        # TRACING #  push!(LEXER_TASK_TIMES, time_ns())
    end
    return nothing
end
