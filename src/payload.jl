#
# ParsedPayload
#

# What we send to the consume! method
struct ParsedPayload{B<:AbstractResultBuffer, C<:AbstractParsingContext}
    row_num::Int
    len::Int
    results::B
    parsing_ctx::C
    chunking_ctx::ChunkingContext
    eols_buffer_index::Int32
end
Base.length(payload::ParsedPayload) = payload.len
last_row(payload::ParsedPayload) = (payload.row_num + length(payload) - 1)


#
# PayloadOrderer
#

# Like a Channel, but when you take! a Payload from it, it will be the next one in order
mutable struct PayloadOrderer{B<:AbstractResultBuffer, C<:AbstractParsingContext}
    queue::Channel{ParsedPayload{B,C}}
    expected_row::Int
    waititng_room::Vector{ParsedPayload{B,C}}
end
PayloadOrderer(queue::Channel) = PayloadOrderer(queue, 1, sizehint!(ParsedPayload[], Threads.nthreads()))
PayloadOrderer() = PayloadOrderer(Channel{ParsedPayload}(Inf), 1, sizehint!(ParsedPayload[], Threads.nthreads()))

function _reenqueue_ordered!(queue::Channel{T}, waiting_room::Vector{T}, payload::T) where {T}
    row = payload.row_num
    for _ in 1:length(waiting_room)
        nrows = length(payload)
        payload = first(waiting_room)
        if payload.row_num == (nrows + row)
            put!(queue, popfirst!(waiting_room))
        else
            break
        end
    end
end

function insertsorted!(arr::Vector{T}, x::T, by=identity) where {T}
    idx = searchsortedfirst(arr, x, by=by)
    insert!(arr, idx, x)
    return idx
end

function _reorder!(queue::Channel{T}, waiting_room::Vector{T}, payload::T, expected_row::Int) where{T}
    row = payload.row_num
    if row == expected_row
        _reenqueue_ordered!(queue, waiting_room, payload)
        return false
    end

    insertsorted!(waiting_room, payload, x->x.row_num)

    if waiting_room[1].row_num == expected_row
        payload = popfirst!(waiting_room)
        put!(queue, payload)
        _reenqueue_ordered!(queue, waiting_room, payload)
    end

    return true
end

Base.put!(o::PayloadOrderer, x::ParsedPayload) = put!(o.queue, x)
Base.close(o::PayloadOrderer, excp::Exception=Base.closed_exception()) = close(o.queue, excp)

function Base.take!(o::PayloadOrderer) # Blocks until a the next payload in order arrives
    payload = take!(o.queue)
    while _reorder!(o.queue, o.waititng_room, payload, o.expected_row)
        payload = take!(o.queue)
    end
    o.expected_row = last_row(payload) + 1
    return payload
end
