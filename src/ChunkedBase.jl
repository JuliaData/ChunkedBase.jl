module ChunkedBase

using Mmap
using CodecZlibNG

using NewlineLexers
using SentinelArrays.BufferedVectors

# The means through which the user can provide their own parsing logic.
include("ParsingContexts.jl")

# A counter-based synchronization primitive that is used to coordinate the parsing/consuming tasks.
include("TaskCounters.jl")
using .TaskCounters

# The `ChunkingContext` is used to keep track of the current chunk of data being processed
# and to coordinate with the parsing/consuming tasks. Two `ChunkingContext` objects are used
# to double-buffer the input.
include("ChunkingContext.jl")

# The `ParsedPayload` is used to pass the results of parsing to the `consume!` method. All relevant
# information about the current chunk of data being processed is passed to `consume!` via `ParsedPayload`.
# In case the parsed results need to be consumed in order, the `PayloadOrderer` can be used to
# sort the payloads.
include("payload.jl")

# Consuming is the means through which the parsed results are used by the end user.
include("ConsumeContexts.jl")
using .ConsumeContexts

# By lexing the input in advance we can detect that we got to an inconsistent state
# (e.g. an unmatched quote at the end of the file) in which case we throw one of these exceptions.
include("exceptions.jl")

# Utilities for the coordinator task to handle the input and the newline positions.
include("read_and_lex_utils.jl")
include("read_and_lex.jl")

# The main entrypoints of the library.
include("parser_parallel.jl")
include("parser_serial.jl")

export ChunkingContext, tasks_per_chunk, total_result_buffers_count
export AbstractParsingContext
export AbstractConsumeContext, setup_tasks!, consume!, task_done!, cleanup
export AbstractResultBuffer
export TaskCounter
export ParsedPayload, PayloadOrderer
export SkipContext
export Lexer
export parse_file_serial, parse_file_parallel, populate_result_buffer!


# By uncommenting all `# TRACING #` in the package, e.g. by turning them to `#= ... =#`,
# you'll enable low-overhead tracing capability.
# Before parsing, call `clear_traces!` to reset the traces, then call
#   include("_tracing.jl") # once
#   plot_traces() # to plot the traces
# TODO: Port this over to be macro-based, with plotting being provided by a package extension

# TRACING # const PARSER_TASKS_TIMES = [UInt[]]
# TRACING # const CONSUMER_TASKS_TIMES = [UInt[]]
# TRACING # const IO_TASK_TIMES = UInt[]
# TRACING # const LEXER_TASK_TIMES = UInt[]
# TRACING # const T1 = UInt[]
# TRACING # const T2 = UInt[]
# TRACING # get_parser_task_trace(i) = PARSER_TASKS_TIMES[i]
# TRACING # get_consumer_task_trace(i) = CONSUMER_TASKS_TIMES[i]
# TRACING # function clear_traces!(nworkers::Int=Threads.nthreads())
# TRACING #     for _ in (length(PARSER_TASKS_TIMES)+1:nworkers)
# TRACING #         push!(PARSER_TASKS_TIMES, UInt[])
# TRACING #     end
# TRACING #     empty!(IO_TASK_TIMES)
# TRACING #     empty!(LEXER_TASK_TIMES)
# TRACING #     empty!(T1)
# TRACING #     empty!(T2)
# TRACING #     foreach(empty!, PARSER_TASKS_TIMES)
# TRACING #     foreach(empty!, CONSUMER_TASKS_TIMES)
# TRACING #     return nothing
# TRACING # end
# TRACING # function dump_traces(path)
# TRACING #     open(path, "w") do io
# TRACING #         write(io, UInt32(length(IO_TASK_TIMES)), IO_TASK_TIMES)
# TRACING #         write(io, UInt32(length(LEXER_TASK_TIMES)), LEXER_TASK_TIMES)
# TRACING #         write(io, UInt32(length(T1)), T1)
# TRACING #         write(io, UInt32(length(T2)), T2)
# TRACING #
# TRACING #         write(io, UInt32(length(PARSER_TASKS_TIMES)))
# TRACING #         for x in PARSER_TASKS_TIMES
# TRACING #             write(io, UInt32(length(x)), x)
# TRACING #         end
# TRACING #         write(io, UInt32(length(CONSUMER_TASKS_TIMES)))
# TRACING #         for x in CONSUMER_TASKS_TIMES
# TRACING #             write(io, UInt32(length(x)), x)
# TRACING #         end
# TRACING #     end
# TRACING #     return nothing
# TRACING # end
# TRACING # _resize!(vv, n) = length(vv) >= n ? resize!(vv, n) : append!(vv, [UInt[] for _ in 1:n-length(vv)])
# TRACING # function load_traces!(path)
# TRACING #     open(path, "r") do io
# TRACING #         read!(io, resize!(IO_TASK_TIMES, read(io, UInt32)))
# TRACING #         read!(io, resize!(LEXER_TASK_TIMES, read(io, UInt32)))
# TRACING #         read!(io, resize!(T1, read(io, UInt32)))
# TRACING #         read!(io, resize!(T2, read(io, UInt32)))
# TRACING #
# TRACING #         _resize!(PARSER_TASKS_TIMES, read(io, UInt32))
# TRACING #         for x in PARSER_TASKS_TIMES
# TRACING #             read!(io, resize!(x, read(io, UInt32)))
# TRACING #         end
# TRACING #         _resize!(CONSUMER_TASKS_TIMES, read(io, UInt32))
# TRACING #         for x in CONSUMER_TASKS_TIMES
# TRACING #             read!(io, resize!(x, read(io, UInt32)))
# TRACING #         end
# TRACING #     end
# TRACING #     return nothing
# TRACING # end

end
