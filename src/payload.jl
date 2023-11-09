#
# ParsedPayload
#

"""
    ParsedPayload{B, C<:AbstractParsingContext}

A payload of parsed results, which is passed to `consume!` after each `populate_result_buffer!` call.

# Fields:
- `row_num::Int`: row number of the first row in the payload
- `len::Int`: number of rows in the payload
- `results::B`: parsed result buffer
- `parsing_ctx::C`: library-provided data (to distinguish JSONL and CSV processing)
- `chunking_ctx::ChunkingContext`: contains the raw bytes, synchronization objects and newline positions
- `eols_buffer_index::Int32`: The start index of the newline positions in `chunking_ctx.newline_positions` that this payload corresponds to.

# See also:
- [`consume!`](@ref), [`AbstractParsingContext`](@ref), [`ChunkingContext`](@ref), [`AbstractResultBuffer`](@ref), [`PayloadOrderer`](@ref)
"""
struct ParsedPayload{B, C<:AbstractParsingContext}
    row_num::Int                  # row number of the first row in the payload
    len::Int                      # number of rows in the payload
    results::B                    # parsed result buffer
    parsing_ctx::C                # library-provided data (to distinguish JSONL and CSV processing)
    chunking_ctx::ChunkingContext # internal data to facilitate chunking and synchronization
    eols_buffer_index::Int32      # The start index of the newline positions in `chunking_ctx.newline_positions` that this payload corresponds to.
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


"""
    PayloadOrderer{B, C<:AbstractParsingContext} <: AbstractChannel{ParsedPayload{B,C}}

A channel-like object that ensures that the payloads are consumed in order.

To use the `PayloadOrderer` you should create your own `AbstractConsumeContext` that contains it
and override the `consume!` to only `put!` payloads in the `PayloadOrderer` and `take!` payloads
from it using a separate task. For example:

```
struct MyConsumeContext <: AbstractConsumeContext
    orderer::PayloadOrderer{MyResultBuffer, MyParsingContext}
end

# Forward the payloads, which will arrive in random order, to the orderer
function ChunkedBase.consume!(consume_ctx::MyConsumeContext, payload::ParsedPayload)
    put!(consume_ctx.orderer, payload)
end

# Expect `ChunkedBase.task_done!` to be called twice per payload.
# By default, we'd do it once after every `consume!`
# But we'll add another one in the task that takes from the orderer.
# This will make sure that the current chunk won't ger recycled after our task is done with it.
function ChunkedBase.setup_tasks!(::MyConsumeContext, chunking_ctx::ChunkingContext, ntasks::Int)
    set!(chunking_ctx.counter, 2*ntasks)
end

consume_ctx = MyConsumeContext(PayloadOrderer{MyResultBuffer, MyParsingContext}())

# Our task that needs to process the payloads in order
@spawn begin
    while true
        payload = take!(consume_ctx.orderer)
        do_something_that_requires_ordered_results(payload)
        task_done!(consume_ctx, payload.chunking_ctx)
    end
end

parse_file_parallel(lexer, parsing_ctx, consume_ctx, chunking_ctx, result_buf)
```
NOTE: It is not safe to call `take!` from multiple tasks on a `PayloadOrderer`.

# See also:
- [`ParsedPayload`](@ref)
"""
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
