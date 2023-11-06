# ChunkedBase.jl
<!--
The package provides a foundational structure for parallel processing of chunked data. It handles ingestion of data chunks and the distribution/synchronization
of work that happens on these chunks. It is designed to be extended by other packages, such as `ChunkedCSV.jl` and `ChunkedJSONL.jl`, who control *how* the data is parsed and consumed. -->

The package handles ingestion of data chunks and the distribution & synchronization of work that happens on these chunks in parallel. It came to existence while refactoring the `ChunkedCSV.jl` and `ChunkedJSON.jl` packages and was designed to by extended by packages like these. It is a package used to write parser packages.

Specifically, `ChunkedBase.jl` spawns one task which handles IO and behaves as a coordinator, and a configurable number of worker tasks.
Both CSV and JSONL are textual formats which delimit records by newlines which makes newlines an ideal point to distribute work. One the coordinator task we ingest bytes into preallocated buffer and then use `NewlineLexers.jl` package to quickly find newlines in it. In turn these newlines are distributed among worker tasks and the coordinator immediately starts working a secondary buffer, while the first one is being processed by the workers.

The process looks something like this, with the coordinator task at the bottom:

| ![Diagram](/docs/diagrams/chunked_base.png) |
|:--:|
| *The coordinator synchronizes with workers using a counter behind a mutex (there is one per buffer). It splits the newlines into N segments, distributes them and increments the counter by N. After the coordinator distributes work, it starts to process the second chunk of data, while the first buffer is being worked on. There is handoff happening between the two buffers -- we need to copy the bytes after the last newline from the first buffer to the second. Each worker decrements after the `consume!` is done, and the coordinator will wait for the counter to reach 0 before it overwrites the buffer with new data.* |

Packages like `ChunkedCSV.jl` and `ChunkedJSON.jl` hook into this structure by defining their own `populate_result_buffer!` methods that parse the records they were assigned into their custom `Result` buffers which are then handed to the `consume!` method (e.g. to be inserted to database).

The main entry point of this package is the `parse_file_parallel` function, which accepts a number of "context" arguments, each controlling different aspect of the process:
```julia
function parse_file_parallel(
    lexer::NewlineLexers.Lexer,
    parsing_ctx::AbstractParsingContext,
    consume_ctx::AbstractConsumeContext,
    chunking_ctx::ChunkingContext,
    result_buffers::Vector{<:AbstractResultBuffer},
    ::Type{CT}=Tuple{} # ignore this for now
) where {CT}
```
Let's break it down:
* `lexer` controls how we find newlines in the ingested chunks of data. Newlines serve as record delimiters and knowing their positions allows us to split work safely among multiple workers, which are spawned internally. `Lexer`s are defined in the `NewlineLexers.jl` package.
* `parsing_ctx` controls how we parse the data. It allows the user to dispatch on custom `populate_result_buffer!` overload and to forward configurations to it. `populate_result_buffer!` is where we take the records identified by the `lexer` and parse them into `result_buffers`.
* `consume_ctx` controls how the parsed results are consumed (e.g. inserted them into a database, appended to a `DataFrame`...). `consume_ctx` allows the user to dispatch on their `consume!` method and hold any state necessary for consuming. This happens immediately after `populate_result_buffer!`.
* `chunking_ctx` controls how the work on individual chunks of data is scheduled. It contains buffers for input bytes and found newlines. Through this struct the user controls the size of the chunks and number spawned tasks that carry out the parsing and consuming. If there is enough data in the input, a secondary `chunking_ctx` is created internally to facilitate the double-buffering described above.
* `result_buffers` controls in which format the results are stored. These result buffers hold results from `populate_result_buffer!` and are passed to `consume!`. This allows the user to have multiple result formats for the with `parsing_ctx` e.g. row oriented vs column oriented buffers.

See the docstring of `populate_result_buffer!` and `consume!` for more information about how to integrate with them.

## Example: Examining the results of the Lexer

Since `ChunkedBase.jl` handles newline detection automatically, a very simple chunked processor could just show the newline positions to the user. Of course, this is not very useful apart from demonstrating how to use this package.

```julia
using ChunkedBase
using NewlineLexers: Lexer

# Our chunked processor doesn't require any settings nor does it need to maintain
# any additional state we'll define our `ParsingContext` and `ConsumeContext` only
# for dispatch reasons.
struct ParsingContext <: AbstractParsingContext end
struct ConsumeContext <: AbstractConsumeContext end
# Our buffer will hold the newline positions in the current chunk
struct ResultBuffer <: AbstractResultBuffer
    newlines_in_chunk::Vector{Int}
end

# Our overload copies `newlines_segment` to our result buffer
function ChunkedBase.populate_result_buffer!(
    result_buf::ResultBuffer,
    newlines_segment::AbstractVector{Int32},
    ::ParsingContext,
    ::Vector{UInt8},
    ::Union{Nothing,Vector{UInt8}}=nothing,
    ::Type=Tuple{}
)
    resize!(result_buf.newlines_in_chunk, length(newlines_segment))
    result_buf.newlines_in_chunk .= copy(newlines_segment)
    return nothing
end

# We consume result buffer by simply printing it
function ChunkedBase.consume!(::ConsumeContext, payload::ParsedPayload)
    chunking_ctx = payload.chunking_ctx
    result_buffer = payload.results

    # To demonstrate how double-buffering works, print the buffer id and the refill number
    chunk_info = (chunking_ctx.id, chunking_ctx.buffer_refills[])
    @info "Newlines in chunk (id:$chunk_info): $(result_buffer.newlines_in_chunk)"
    return nothing
end
```

Now that we defined all overloads and contexts we can define our user-facing function:

```julia
# Prints relative positions of `\n` in each ingested chunk of size `buffersize`,
# using `nworkers` spawned tasks.
function print_newlines(io, buffersize, nworkers)
    lexer = Lexer(io, nothing, '\n')
    parsing_ctx = ParsingContext()
    consume_ctx = ConsumeContext()
    chunking_ctx = ChunkingContext(buffersize, nworkers, 0, nothing)
    # ChunkedBase.jl requires 2 result buffers per worker task, we'd get an error otherwise
    result_buffers = [ResultBuffer(Int[]) for _ in 1:2nworkers]
    # We need to fill the byte buffer and find the newlines in it before we hand
    # the `consume_ctx` to the function
    ChunkedBase.read_and_lex!(lexer, chunking_ctx)
    parse_file_parallel(lexer, parsing_ctx, consume_ctx, chunking_ctx, result_buffers)
    return nothing
end
```
Let's run in on some data:
```julia
julia> io = IOBuffer((("x" ^ 4095) * "\n") ^ 64); # 256KiB
julia> print_newlines(io, 64*1024, 4);
# [ Info: Newlines in chunk (id:(1, 1)): [16384, 20480, 24576, 28672, 32768, 36864]
# [ Info: Newlines in chunk (id:(1, 1)): [0, 4096, 8192, 12288, 16384]
# [ Info: Newlines in chunk (id:(1, 1)): [36864, 40960, 45056, 49152, 53248, 57344]
# [ Info: Newlines in chunk (id:(1, 1)): [57344, 61440, 65536]
# [ Info: Newlines in chunk (id:(2, 1)): [16384, 20480, 24576, 28672, 32768, 36864]
# [ Info: Newlines in chunk (id:(2, 1)): [0, 4096, 8192, 12288, 16384]
# [ Info: Newlines in chunk (id:(2, 1)): [36864, 40960, 45056, 49152, 53248, 57344]
# [ Info: Newlines in chunk (id:(2, 1)): [57344, 61440, 65536]
# [ Info: Newlines in chunk (id:(1, 2)): [0, 4096, 8192, 12288, 16384]
# [ Info: Newlines in chunk (id:(1, 2)): [36864, 40960, 45056, 49152, 53248, 57344]
# [ Info: Newlines in chunk (id:(1, 2)): [16384, 20480, 24576, 28672, 32768, 36864]
# [ Info: Newlines in chunk (id:(1, 2)): [57344, 61440, 65536]
# [ Info: Newlines in chunk (id:(2, 2)): [0, 4096, 8192, 12288, 16384]
# [ Info: Newlines in chunk (id:(2, 2)): [16384, 20480, 24576, 28672, 32768, 36864]
# [ Info: Newlines in chunk (id:(2, 2)): [36864, 40960, 45056, 49152, 53248, 57344]
# [ Info: Newlines in chunk (id:(2, 2)): [57344, 61440, 65536]
```
Behind the scenes, `ChunkedBase.jl` was using two 64KiB buffers, finding newlines in them, and splitting the found newlines among 4 tasks. We can see that each of the buffers (identified by the first number in `id` tuple) was refilled two times (refill number is the second element of the tuple).
The way we setup our data, there should be one newline every 4KiB of input, so we'd expect 16 newlines per chunk, but we could see that there are 20 numbers reported per chunk -- this is because the first element of the newline segment we send to the tasks is either 0 or the end of the previous sub-chunk, so we get 4 duplicated elements.

