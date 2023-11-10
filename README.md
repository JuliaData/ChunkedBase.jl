# ChunkedBase.jl

The package handles the ingestion of data chunks and the distribution & synchronization of work that happens on these chunks in parallel. It came into existence while refactoring the [`ChunkedCSV.jl`](https://github.com/RelationalAI/ChunkedCSV.jl) and [`ChunkedJSONL.jl`](https://github.com/RelationalAI/ChunkedJSONL.jl) packages and was designed to be extended by packages like these. It is a package used to write parser packages.

## Overview

The purpose of the package is to provide a framework for creating parsers for textual formats, where each record is separated by a newline character. Examples of such formats include CSV and JSONLines. The package is designed to operate on data chunks and to reuse preallocated buffers for each of these chunks. It simplifies the process of ingesting data, locating newlines, distributing work among multiple tasks, and synchronizing them. To implement a parsing package, one only needs to define how to *parse* the records and how to *consume* the parsed results. 

Parsing refers to taking the raw input bytes and the position of newlines that will be available to the user, and using a package like `Parsers.jl` or `JSON3.jl`, or some custom parsing code to produce records of Julia types that could be stored in user-defined result buffers, which can be customized e.g for row-oriented or column-oriented storage.

Consuming refers to taking the parsed results and doing something with them, such as inserting them into a database, appending them to a `DataFrame`, or writing them to a file.

## How it works

Internally, `ChunkedBase.jl` uses a coordinator task and a set number of worker tasks to process data. The coordinator task is responsible for handling IO operations and acts as a coordinator for the worker tasks. It reads bytes from an input buffer and uses the [`NewlineLexers.jl`](https://github.com/JuliaData/NewlineLexers.jl) package to identify newlines in the data. Since the data is delimited by newlines, these newlines are used to split up the work among the worker tasks. 

The coordinator task alternates between two buffers, processing one while the workers process the other. This ensures that there is always data available for the workers to consume, which maximizes throughput across all tasks. We call this technique double-buffering.

The process looks something like this, with the coordinator task at the bottom:

| ![Diagram](/docs/diagrams/chunked_base.png) |
|:--:|
| *The coordinator uses a counter behind a mutex to synchronize with the workers. There is one such counter per buffer, and the counter is incremented by N after the coordinator splits the newlines into N segments and distributes them. After distributing the work, the coordinator starts to process the second chunk of data while the first buffer is still being worked on. A handoff occurs between the two buffers, where the bytes after the last newline from the first buffer are copied to the second. Each worker decrements the counter after completing the `consume!` operation, and the coordinator waits for the counter to reach 0 before overwriting the buffer with new data.* |

Packages like `ChunkedCSV.jl` and `ChunkedJSONL.jl` hook into this structure by defining their own `populate_result_buffer!` methods that parse the records they were assigned into their custom `Result` buffers which are then handed to the `consume!` method (e.g. to be inserted into a database).

The main entry point of this package is the `parse_file_parallel` function, which accepts several "context" arguments, each controlling a different aspect of the process:
```julia
function parse_file_parallel(
    lexer::Lexer,
    parsing_ctx::AbstractParsingContext, # user-defined
    consume_ctx::AbstractConsumeContext, # user-defined
    chunking_ctx::ChunkingContext,
    result_buffers::Vector{<:AbstractResultBuffer}, # user-defined
    ::Type{CT}=Tuple{} # ignore this for now
) where {CT}
```
Let's break it down:
* `lexer` controls how we find newlines in the ingested chunks of data. Newlines serve as record delimiters and knowing their positions allows us to split work safely among multiple workers, which are spawned internally. `Lexer`s are defined in the `NewlineLexers.jl` package.
* `parsing_ctx` controls how we parse the data. It allows the user to dispatch on custom `populate_result_buffer!` overload and to forward configurations to it. `populate_result_buffer!` is where we take the records identified by the `lexer` and parse them into `result_buffers`.
* `consume_ctx` controls how the parsed results are consumed (e.g. inserted into a database, appended to a `DataFrame`...). `consume_ctx` allows the user to dispatch on their `consume!` method and hold any state necessary for consumption. This happens immediately after `populate_result_buffer!`.
* `chunking_ctx` controls how the work on individual chunks of data is scheduled. It contains buffers for input bytes and found newlines. Through this struct the user controls the size of the chunks and the number of spawned tasks that carry out the parsing and consuming. If there is enough data in the input, a secondary `chunking_ctx` is created internally to facilitate the double-buffering described above.
* `result_buffers` controls in which format the results are stored. These result buffers hold results from `populate_result_buffer!` and are passed to `consume!`. This allows the user to have multiple result formats for the same `parsing_ctx` e.g. row-oriented vs column-oriented buffers.

There is also `parse_file_serial` which doesn't spawn any tasks and just calls `populate_result_buffer!` and `consume!` sequentially without double-buffering. This can be useful for debugging or for small files.

See the docstring of `populate_result_buffer!` and `consume!` for more information about how to integrate with them.

## Example: Examining the results of the Lexer

Since `ChunkedBase.jl` handles newline detection automatically, a very simple chunked processor could just show the newline positions to the user. Of course, this is not very useful apart from demonstrating how to use this package.

```julia
using ChunkedBase

# Our chunked processor doesn't require any settings nor does it need to maintain
# any additional state, so we'll define our `ParsingContext` and `ConsumeContext` only
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
    result_buf.newlines_in_chunk .= newlines_segment
    return nothing
end

# We consume result buffer by simply printing it
function ChunkedBase.consume!(::ConsumeContext, payload::ParsedPayload)
    # The ParsedPayload wraps the result buffer and the other contexts
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
    chunking_ctx = ChunkingContext(buffersize, nworkers)
    # ChunkedBase.jl requires 2 result buffers per worker task, we'd get an error otherwise
    result_buffers = [ResultBuffer(Int[]) for _ in 1:2nworkers]

    parse_file_parallel(lexer, parsing_ctx, consume_ctx, chunking_ctx, result_buffers)
    return nothing
end
```
Let's run it on some data:
```julia
julia> io = IOBuffer((("x" ^ 4095) * "\n") ^ 64); # 256KiB
julia> print_newlines(io, 64 * 1024, 4);
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
Behind the scenes, `ChunkedBase.jl` was using two 64KiB buffers, finding newlines in them, and splitting the found newlines among 4 tasks. We can see that each of the buffers (identified by the first number in the `id` tuple) was refilled two times (the refill number is the second element of the tuple). 
The way we set up our data, there should be one newline every 4KiB of input, so we'd expect 16 newlines per chunk, but we could see that there are 20 numbers reported per chunk -- this is because each newline segment we send to the tasks starts with the last newline position from the previous segment or 0 for the first segment, so we get 4 duplicated elements in this case.

## Advanced usage

### Customizing work coordination

Internally, the coordination of work happens through `setup_tasks!`, `task_done!` and `sync_tasks` functions which are defined in the `ConsumeContexts.jl` file. `setup_tasks!` sets the counter for the number of units of work that are expected to happen on the current chunk. `task_done!` is called by the worker tasks when they are done with their work, which will decrement the counter. `sync_tasks` is called by the coordinator task to wait for all workers to finish their work, i.e. for the counter to be 0. These functions are exposed to the user through the `AbstractConsumeContext` interface, so they can be overloaded to implement custom synchronization strategies. For example, if the user wants to send the parsed result buffers to external tasks from their `consume!` method and wait for *them* to be done with their work, they can overload `setup_tasks!` to increase the expected number of units of work and then call `task_done!` from the external tasks when they are done. This way, the coordinator task will wait for the external tasks to finish their work before refilling the buffer with new data. See the docstrings of these functions for more information.

### Sniffing the beginning of the file

Sometimes we want to skip over the first couple of lines of a file, e.g. when they contain comments or metadata, or we might want to set up our `AbstractParsingContext` with some information that is available at the beginning of the file (like the header names of a CSV).

To do this, we can use fill our `ChunkingContext` with `read_and_lex!` which will read the first `buffersize` bytes from the input and lex them so that we can inspect the newlines and decide what to do with the file. We can use `skip_rows_init!` to skip over rows that we don't want to parse, and then use `parse_file_parallel` to parse the rest of the file, so our `populate_result_buffers!` method can focus on the "cleaner" part of the file.

Instead of `read_and_lex!`, one could also call `initial_read!` and `initial_lex!` separately. This gives you the opportunity to detect which newline type is used in the file and set up your lexer accordingly.
