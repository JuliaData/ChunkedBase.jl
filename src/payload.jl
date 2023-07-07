#
# ParsedPayload
#

# What we send to the consume! method
struct ParsedPayload{C<:AbstractParsingContext}
    row_num::Int
    len::Int
    parsing_ctx::C
    eols_buffer_index::Int32
end
Base.length(payload::ParsedPayload) = payload.len
last_row(payload::ParsedPayload) = payload.row_num + length(payload) - 1

#
# PayloadOrderer
#

# Like a Channel, but when you take! a Payload from it, it will be the next one in order
mutable struct PayloadOrderer{C<:AbstractParsingContext} <: AbstractChannel{ParsedPayload{C}}
    queue::Channel{ParsedPayload{C}}
    expected_row::Int
    waititng_room::Vector{ParsedPayload{C}}
end
PayloadOrderer(queue::Channel{ParsedPayload{C}}) where {C} = PayloadOrderer{C}(queue)
PayloadOrderer{C}(queue::Channel{ParsedPayload{C}}) where {C} = PayloadOrderer{C}(queue, 1, sizehint!(ParsedPayload{C}[], Threads.nthreads()))
PayloadOrderer{C}() where {C} = PayloadOrderer{C}(Channel{ParsedPayload{C}}(Inf), 1, sizehint!(ParsedPayload{C}[], Threads.nthreads()))

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
    return true
end

Base.put!(o::PayloadOrderer{C}, x::ParsedPayload{C}) where {C} = put!(o.queue, x)
Base.close(o::PayloadOrderer, excp::Exception=Base.closed_exception()) = close(o.queue, excp)
Base.isopen(o::PayloadOrderer) = isopen(o.queue)

function Base.take!(o::PayloadOrderer) # Blocks until a the next payload in order arrives
    payload = take!(o.queue)
    while _reorder!(o.queue, o.waititng_room, payload, o.expected_row)
        payload = take!(o.queue)
    end
    o.expected_row = last_row(payload) + 1
    return payload
end
