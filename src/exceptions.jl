abstract type FatalLexingError <: Exception end
Base.showerror(io::IO, e::FatalLexingError) = print(io, e.msg)

# TODO(#12): Add some data to help debug the problematic file, like the first row with an escape character
# and/or the quote character.
struct NoValidRowsInBufferError <: FatalLexingError
    msg::String
    buffersize::Int

    function NoValidRowsInBufferError(buffersize)
        return new(
            string(
                "Parsing job failed on lexing newlines. There was no valid newline in the ",
                "entire buffer of $(Base.format_bytes(buffersize)). ",
                "This could happen if your buffer is too small, or if your quote or escape ",
                "characters are not setup correctly.",
            ),
            buffersize,
        )
    end
end

struct UnmatchedQuoteError <: FatalLexingError
    msg::String

    function UnmatchedQuoteError()
        new(
            string(
                "Parsing job failed on lexing newlines. The file has ended with an unmatched quote. ",
                "This could happen if your quote or escape characters are not set properly.",
            ),
        )
    end
end
