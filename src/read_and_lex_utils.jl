function _detect_newline(buf, pos, len)
    len == 0 && return UInt8('\n') # empty file
    @assert 1 <= pos <= len <= length(buf)

    v = view(buf, pos:len)
    if isnothing(findfirst(==(UInt8('\n')), v))
        if isnothing(findfirst(==(UInt8('\r')), v))
            throw(ArgumentError("No newline detected. Specify the newline character explicitly via the `newline` keyword argument. Use `\n` even for CRLF."))
        else
            return UInt8('\r')
        end
    else
        return UInt8('\n')
    end
end

function _hasBOM(bytes::Vector{UInt8})
    return @inbounds bytes[1] == 0xef && bytes[2] == 0xbb && bytes[3] == 0xbf
end

function _isemptyrow(prev_nl, next_nl, bytes)
    return prev_nl + 1 == next_nl || (prev_nl + 2 == next_nl && @inbounds(bytes[prev_nl+1]) == UInt8('\r'))
end

_nonspace(b::UInt8) = !(b == UInt8(' ') || UInt8('\t') <= b <= UInt8('\r'))

#
# _input_to_io
#

_input_to_io(input::IO, use_mmap::Bool) = false, input
function _input_to_io(input::String, use_mmap::Bool)
    ios = open(input, "r")
    if !eof(ios) && peek(ios, UInt16) == 0x8b1f
        # TODO: GzipDecompressorStream doesn't respect MmapStream reaching EOF for some reason
        # io = CodecZlibNG.GzipDecompressorStream(use_mmap ? MmapStream(ios) : ios)
        use_mmap && @warn "`use_mmap=true` is currently unsupported when reading gzipped files, using file io."
        io = CodecZlibNG.GzipDecompressorStream(ios)
    elseif use_mmap
        io = MmapStream(ios)
    else
        io = ios
    end
    return true, io
end

#
# limit_eols!
#

function limit_eols!(chunking_ctx::ChunkingContext, row_num::Int)
    _limit = chunking_ctx.limit
    _limit == 0 && return false
    if row_num > _limit
        return true
    elseif row_num <= _limit < row_num + length(chunking_ctx.newline_positions) - 1
        chunking_ctx.newline_positions.occupied -= (row_num + length(chunking_ctx.newline_positions) - 1) - _limit - 1
    end
    return false
end

#
# Initial row skipping
#

function _startswith(s::AbstractVector{UInt8}, soff::Integer, prefix::AbstractVector{UInt8})
    length(s) - soff < length(prefix) && return false
    @inbounds for i in eachindex(prefix)
        s[i + soff] == prefix[i] || return false
    end
    return true
end
_startswith(s::AbstractVector{UInt8}, prefix::AbstractVector{UInt8}) = _startswith(s, 0, prefix)
_startswith(s, soff, prefix::Nothing) = false
_startswith(s, prefix::Nothing) = false

skip_rows_init!(lexer, chunking_ctx, rows_to_skip) = _skip_rows_init!(lexer, chunking_ctx, rows_to_skip, chunking_ctx.comment)

function _skip_rows_init!(lexer, chunking_ctx, rows_to_skip, comment)
    input_is_empty = length(chunking_ctx.newline_positions) == 1
    lines_skipped_total = 0
    input_is_empty && return lines_skipped_total
    # To know where in the end-of-line buffer we are while deciding whether we can skip or
    # if we need to refill the buffer because we skipped everything in it.
    eol_index = 1
    @inbounds while true
        # Did we exhaust the buffer during skipping?
        if eol_index == length(chunking_ctx.newline_positions)
            if lexer.done
                break
            else
                ChunkedBase.read_and_lex!(lexer, chunking_ctx)
                eol_index = 1
            end
        end

        if !_startswith(chunking_ctx.bytes, chunking_ctx.newline_positions[eol_index], comment)
            # Non commented row: if we still have rows to skip, we skip it, otherwise we're done
            if rows_to_skip > 0
                rows_to_skip -= 1
            else
                break
            end
        # Commented row: we skip it if we still have rows to skip, otherwise we move to another row.
        # This means that if there are consecutive commented rows after the rows to skip, we'll
        # skip them all.
        else rows_to_skip > 0
            rows_to_skip -= 1
        end
        eol_index += 1
        lines_skipped_total += 1
    end

    # We need to shift the newline positions to the left to account for the skipped rows
    shiftleft!(chunking_ctx.newline_positions, eol_index-1)
    return lines_skipped_total
end

function _skip_rows_init!(lexer, chunking_ctx, rows_to_skip, comment::Nothing)
    # If there are more rows to skip than the number of rows in the buffer, we skip the whole buffer
    while !lexer.done && rows_to_skip >= length(chunking_ctx.newline_positions) - 1
        rows_to_skip -= length(chunking_ctx.newline_positions) - 1
        ChunkedBase.read_and_lex!(lexer, chunking_ctx)
    end
    rows_to_skip = min(rows_to_skip, length(chunking_ctx.newline_positions) - 1)
    # We need to shift the newline positions to the left to account for the skipped rows
    shiftleft!(chunking_ctx.newline_positions, rows_to_skip)
    return rows_to_skip
end

#
# MmapStream
#

mutable struct MmapStream <: IO
    ios::IOStream
    x::Vector{UInt8}
    pos::Int
end
MmapStream(ios::IO) = MmapStream(ios, Mmap.mmap(ios, grow=false, shared=false), 1)
Base.close(m::MmapStream) = close(m.ios)
Base.eof(m::MmapStream) = m.pos == length(m.x)
function readbytesall!(io::MmapStream, buf, n::Int)
    bytes_to_read = min(bytesavailable(io), n)
    unsafe_copyto!(pointer(buf), pointer(io.x) + io.pos - 1, bytes_to_read)
    io.pos += bytes_to_read
    return bytes_to_read
end
# Interop with GzipDecompressorStream
Base.bytesavailable(m::MmapStream) = length(m.x) - m.pos
Base.isopen(m::MmapStream) = isopen(m.ios) && !eof(m)
Base.filesize(io::MmapStream) = length(io.x)
function Base.unsafe_read(from::MmapStream, p::Ptr{UInt8}, nb::UInt)
    avail = bytesavailable(from)
    adv = min(avail, nb)
    GC.@preserve from unsafe_copyto!(p, pointer(from.x) + from.pos - 1, adv)
    from.pos += adv
    if nb > avail
        throw(EOFError())
    end
    return nothing
end
function Base.read(io::MmapStream, ::Type{UInt8})
    avail = bytesavailable(io)
    if avail == 0
        throw(EOFError())
    end
    io.pos += 1
    return io.x[io.pos]
end
