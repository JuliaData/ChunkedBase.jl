#
# ParsedPayload
#

# What we send to the consume! method
struct ParsedPayload{B, C<:AbstractParsingContext}
    row_num::Int
    len::Int
    results::B
    parsing_ctx::C
    chunking_ctx::ChunkingContext
    eols_buffer_index::Int32
end
Base.length(payload::ParsedPayload) = payload.len
last_row(payload::ParsedPayload) = payload.row_num + length(payload) - 1


#
# PayloadOrderer
#

# Like a Channel, but when you take! a Payload from it, it will be the next one in order
mutable struct PayloadOrderer{B<:AbstractResultBuffer, C<:AbstractParsingContext} <: AbstractChannel{ParsedPayload{B,C}}
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
            _put_buffered_nolock!(queue, popfirst!(waiting_room))
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
        return false # doesn't need to wait
    end
    insertsorted!(waiting_room, payload, x->x.row_num)
    return true # needs to wait
end

Base.put!(o::PayloadOrderer{B,C}, x::ParsedPayload{B,C}) where {B,C} = put!(o.queue, x)
Base.close(o::PayloadOrderer, excp::Exception=Base.closed_exception()) = close(o.queue, excp)
Base.isopen(o::PayloadOrderer) = isopen(o.queue)

# This is `take_buffered` from channels.jl, with the enclosing lock/unlock calls removed
function _take_buffered_nolock!(c::Channel)
    while isempty(c.data)
        Base.check_channel_state(c)
        wait(c.cond_take)
    end
    v = popfirst!(c.data)
    Base._increment_n_avail(c, -1)
    notify(c.cond_put, nothing, false, false) # notify only one, since only one slot has become available for a put!.
    return v
end

# This is `put_buffered` from channels.jl, with the enclosing lock/unlock calls removed
function _put_buffered_nolock!(c::Channel, v)
    did_buffer = false
    try
        # Increment channel n_avail eagerly (before push!) to count data in the
        # buffer as well as offers from tasks which are blocked in wait().
        Base._increment_n_avail(c, 1)
        while length(c.data) == c.sz_max
            Base.check_channel_state(c)
            wait(c.cond_put)
        end
        Base.check_channel_state(c)
        push!(c.data, v)
        did_buffer = true
        # notify all, since some of the waiters may be on a "fetch" call.
        notify(c.cond_take, nothing, true, false)
    finally
        # Decrement the available items if this task had an exception before pushing the
        # item to the buffer (e.g., during `wait(c.cond_put)`):
        did_buffer || Base._increment_n_avail(c, -1)
    end
    return v
end

# TODO: since we own our put and take methods, we can ditch the waiting room and just use the channls `data` field
function Base.take!(o::PayloadOrderer) # Blocks until a the next payload in order arrives
    lock(o.queue)
    try
        payload = _take_buffered_nolock!(o.queue)
        while _reorder!(o.queue, o.waititng_room, payload, o.expected_row)
            payload = _take_buffered_nolock!(o.queue)
        end
        o.expected_row = last_row(payload) + 1
        return payload
    finally
        unlock(o.queue)
    end
end
