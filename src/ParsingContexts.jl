"""
    populate_result_buffer!(
        result_buf::AbstractResultBuffer,
        newline_segment:AbstractVector{Int32},
        parsing_ctx::AbstractParsingContext,
        bytes::Vector{UInt8},
        comment::Union{Nothing,Vector{UInt8}}=nothing,
        ::Type{CT}=Tuple{}
    ) where {CT}

    Override with your `AbstractParsingContext` to provide custom logic for parsing the input bytes
    in `parsing_ctx.bytes` between the newline positions in `newline_segment` into `result_buf`.
    The method is called from multiple tasks in parallel, each having a different `newline_segment`,
    some sharing the same `parsing_ctx.bytes`. The `result_buf` is only accessed by one task at a time.

    # Arguments:
    * `result_buf`: a user-provided object which is meant to store the parsing results from this function
    * `newline_segment`: a vector of newline positions in `bytes` which delimit the rows of the input.
    * `parsing_ctx`: a user-provided object that is used to dispatch to this method and carry parsing specific config
    * `bytes`: the raw bytes ingested from the input
    * `comment`: the comment prefix to skip, if any
    * `CT`: an optional, compile-time known object which was passed to `parse_file_parallel` / `parse_file_serial`

    # Notes:
    Each consecutive pair of `newline_segment` values defines an exclusive range of bytes in `bytes` which
    constitutes a single row.

    The range needs to be treated as exclusive because we add a fictional newline at the beginning of the chunk
    at position 0 and past the end of the file if it doesn't end on a newline.
    A safe way of processing each row would be e.g.:

    ```
    start_index = first(newline_segment)
    for i in 2:length(newline_segment)
        end_index = newline_segment[i]
        row_bytes = view(bytes, start_index+1:end_index-1) # +/- 1 is needed!

        # ... actually populate the result_buf

        start_index = end_index
    end
    ```
"""
function populate_result_buffer! end

# Users should subtype this to create custom parsing contexts variables which are
# then used in `parse_file_parallel` / `parse_file_serial`, to dispatch on their
# `populate_result_buffer!` method.
abstract type AbstractParsingContext end
# Users should subtype this to create custom result buffer objects to store the
# parsed results in `populate_result_buffer!`.
abstract type AbstractResultBuffer end