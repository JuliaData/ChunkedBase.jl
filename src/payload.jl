#
# ParsedPayload
#

# What we send to the consume! method
struct ParsedPayload{B, C<:AbstractParsingContext}
    row_num::Int                  # row number of the first row in the payload
    len::Int                      # number of rows in the payload
    results::B                    # parsed result buffer
    parsing_ctx::C                # library-provided data (to distinguish JSONL and CSV processing)
    chunking_ctx::ChunkingContext # internal data to facilitate chunking and synchronization
    eols_buffer_index::Int32      # index of the [e]nd-[o]f-[l]ine[s] buffer in the chunking_ctx
end
Base.length(payload::ParsedPayload) = payload.len
last_row(payload::ParsedPayload) = payload.row_num + length(payload) - 1

#
# PayloadOrderer
#

function insertsorted!(arr::Vector{T}, x::T, by=identity) where {T}
    idx = searchsortedfirst(arr, x, by=by)
    insert!(arr, idx, x)
    return idx
end

# Like a Channel, but when you take! a Payload from it, it will be the next one in order
# take! is not threadsafe
mutable struct PayloadOrderer{B, C<:AbstractParsingContext} <: AbstractChannel{ParsedPayload{B,C}}
    queue::Channel{ParsedPayload{B,C}}
    expected_row::Int
    waititng_room::Vector{ParsedPayload{B,C}}
end
PayloadOrderer(queue::Channel{ParsedPayload{B,C}}) where {B,C} = PayloadOrderer{B,C}(queue)
PayloadOrderer{B,C}(queue::Channel{ParsedPayload{B,C}}) where {B,C} = PayloadOrderer{B,C}(queue, 1, sizehint!(ParsedPayload{B,C}[], Threads.nthreads()))
PayloadOrderer{B,C}() where {B,C} = PayloadOrderer{B,C}(Channel{ParsedPayload{B,C}}(Inf), 1, sizehint!(ParsedPayload{B,C}[], Threads.nthreads()))

function _reenqueue_ordered!(queue::Channel{T}, waiting_room::Vector{T}, payload::T) where {T}
    row = payload.row_num
    for _ in 1:length(waiting_room)
        nrows = length(payload)
        payload = first(waiting_room)
        if payload.row_num == (nrows + row)
            put!(queue, popfirst!(waiting_room))
            row = payload.row_num
        else
            break
        end
    end
end

function _reorder!(queue::Channel{T}, waiting_room::Vector{T}, payload::T, expected_row::Int) where{T}
    row = payload.row_num
    if row == expected_row
        _reenqueue_ordered!(queue, waiting_room, payload)
        return false # needs no reordering
    end
    insertsorted!(waiting_room, payload, x->x.row_num)
    return true # needs reordering
end

Base.put!(o::PayloadOrderer{B,C}, x::ParsedPayload{B,C}) where {B,C} = put!(o.queue, x)
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
