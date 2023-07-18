using ChunkedBase
using SentinelArrays: BufferedVector
using Test
using Random
using CodecZlibNG

# ParsingContext ###########################################################################

struct TestParsingContext <: AbstractParsingContext end

# ResultBuffer #############################################################################

struct TestResultBuffer <: AbstractResultBuffer
    newline_positions::BufferedVector{Int32}
end

function ChunkedBase.populate_result_buffer!(result_buf::TestResultBuffer, newlines_segment::AbstractVector{Int32}, ::TestParsingContext, ::Vector{UInt8}, ::Union{Nothing,Vector{UInt8}}=nothing, ::Type{CT}=Tuple{}) where {CT}
    empty!(result_buf.newline_positions)
    n = length(newlines_segment)
    Base.ensureroom(result_buf.newline_positions, n)

    @inbounds result_buf.newline_positions.elements[1:n] .= newlines_segment
    result_buf.newline_positions.occupied = n

    return nothing
end

# ConsumeContext 1  ########################################################################
### TestConsumeContext #####################################################################
struct TestConsumeContext{B,C} <: AbstractConsumeContext
    queue::PayloadOrderer{B,C}
end
ChunkedBase.consume!(ctx::TestConsumeContext, payload::ParsedPayload) = put!(ctx.queue, payload)
ChunkedBase.task_done!(::TestConsumeContext, ::ChunkingContext) = nothing

### TestThrowingContext ####################################################################
struct TestThrowingContext <: AbstractConsumeContext
    tasks::Vector{Task}
    conds::Vector{TaskCounter}
    throw_row::Int
end
TestThrowingContext(throw_row) = TestThrowingContext(Task[], ChunkedBase.TaskCounter[], throw_row)

function ChunkedBase.consume!(ctx::TestThrowingContext, payload::ParsedPayload)
    t = current_task()
    c = payload.chunking_ctx.counter
    c in ctx.conds || push!(ctx.conds, c)
    t in ctx.tasks || push!(ctx.tasks, t)
    payload.row_num >= ctx.throw_row && error("These contexts are for throwing, and that's all what they do")
    sleep(0.01) # trying to get the task off a fast path to claim everything from the parsing queue
    return nothing
end


# ThrowingIO ###############################################################################

# Throws in the last quarter of the buffer
struct ThrowingIO <: IO
    io::IOBuffer
    throw_byte::Int
end
ThrowingIO(s::String) = ThrowingIO(IOBuffer(s), length(s) - cld(length(s), 4))
ChunkedBase.readbytesall!(io::ThrowingIO, buf, n::Int) = io.io.ptr > io.throw_byte ? error("That should be enough data for everyone") : ChunkedBase.readbytesall!(io.io, buf, n)
Base.eof(io::ThrowingIO) = Base.eof(io.io)


# utils ####################################################################################
make_buf(nrows=1) = TestResultBuffer(BufferedVector{Int32}(collect(Int32(1):Int32(nrows)), nrows))
make_payload(c, nrows=1, row_num=0) = ParsedPayload{TestResultBuffer,TestParsingContext}(row_num, nrows, make_buf(nrows), TestParsingContext(), c, row_num)

function test_serial(io; buffersize=8*1024, limit=0, comment=nothing, skipto=0, newline=UInt8('\n'), lexer_args=(nothing,))
    parsing_ctx = TestParsingContext()
    queue = PayloadOrderer{TestResultBuffer,TestParsingContext}()
    consume_ctx = TestConsumeContext(queue)

    chunking_ctx = ChunkingContext(buffersize, 1, limit, comment)
    lexer = Lexer(io, lexer_args..., newline)

    ChunkedBase.read_and_lex!(lexer, chunking_ctx)
    ChunkedBase.skip_rows_init!(lexer, chunking_ctx, skipto)

    result_buf = TestResultBuffer(BufferedVector{Int32}(Int32[0], 1))
    Threads.@spawn begin
        try
            parse_file_serial($lexer, $parsing_ctx, $consume_ctx, $chunking_ctx, $result_buf, Tuple{})
        finally
            close($queue)
        end
    end
    return queue, chunking_ctx.counter
end

function test_parallel(io; buffersize=8*1024, nworkers=2, limit=0, comment=nothing, skipto=0, newline=UInt8('\n'), lexer_args=(nothing,))
    parsing_ctx = TestParsingContext()
    queue = PayloadOrderer{TestResultBuffer,TestParsingContext}()
    consume_ctx = TestConsumeContext(queue)

    chunking_ctx = ChunkingContext(buffersize, nworkers, limit, comment)
    lexer = Lexer(io, lexer_args..., newline)

    ChunkedBase.read_and_lex!(lexer, chunking_ctx)
    ChunkedBase.skip_rows_init!(lexer, chunking_ctx, skipto)

    result_bufs = [TestResultBuffer(BufferedVector{Int32}(Int32[0], 1)) for _ in 1:ChunkedBase.total_result_buffers_count(chunking_ctx)]
    Threads.@spawn begin
        try
            parse_file_parallel($lexer, $parsing_ctx, $consume_ctx, $chunking_ctx, $result_bufs, Tuple{})
        catch e
            @error "error in parse_file_parallel $e"
            rethrow()
        finally
            close($queue)
        end
    end
    return queue, chunking_ctx.counter
end

@testset "e2e newline finder" begin
    q, counter = test_serial(IOBuffer(""))
    @test take!(q).results.newline_positions == Int32[0]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_parallel(IOBuffer(""))
    @test take!(q).results.newline_positions == Int32[0]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_serial(IOBuffer("123456"))
    @test take!(q).results.newline_positions == Int32[0, 7]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_parallel(IOBuffer("123456"))
    @test take!(q).results.newline_positions == Int32[0, 7]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_serial(IOBuffer("1234"), buffersize=4)
    @test take!(q).results.newline_positions == Int32[0, 5]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_parallel(IOBuffer("1234"), buffersize=4)
    @test take!(q).results.newline_positions == Int32[0, 5]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_serial(IOBuffer("123\n45"), buffersize=4)
    @test take!(q).results.newline_positions == Int32[0, 4]
    ChunkedBase.dec!(counter)
    @test take!(q).results.newline_positions == Int32[0, 3]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_parallel(IOBuffer("123\n45"), buffersize=4) # due to doublebuffering, we get 2 results
    @test take!(q).results.newline_positions == Int32[0, 4]
    @test take!(q).results.newline_positions == Int32[0, 3]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_serial(IOBuffer("123\n45"), buffersize=4, limit=1)
    @test take!(q).results.newline_positions == Int32[0, 4]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_parallel(IOBuffer("123\n45"), buffersize=4, limit=1)
    @test take!(q).results.newline_positions == Int32[0, 4]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_serial(IOBuffer("123\n45"), buffersize=4, skipto=1)
    @test take!(q).results.newline_positions == Int32[0, 3]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_parallel(IOBuffer("123\n45"), buffersize=4, skipto=1)
    @test take!(q).results.newline_positions == Int32[0, 3]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_serial(IOBuffer("\xef\xbb\xbf123\n45"), buffersize=4, skipto=1)
    @test take!(q).results.newline_positions == Int32[0, 3]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_parallel(IOBuffer("\xef\xbb\xbf123\n45"), buffersize=4, skipto=1)
    @test take!(q).results.newline_positions == Int32[0, 3]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_serial(IOBuffer("123\n45\n67"), buffersize=4, limit=1, skipto=1)
    @test take!(q).results.newline_positions == Int32[0, 3]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_parallel(IOBuffer("123\n45\n67"), buffersize=4, limit=1, skipto=1)
    @test take!(q).results.newline_positions == Int32[0, 3]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    for comment in ("#", '#', UInt8('#'), [UInt8('#')])
        q, counter = test_serial(IOBuffer("#12\n123\n45\n67"), buffersize=4, limit=1, skipto=2, comment=comment)
        @test take!(q).results.newline_positions == Int32[0, 3]
        ChunkedBase.dec!(counter)
        @test counter.n == 0
        @test Base.n_avail(q.queue) == 0

        q, counter = test_parallel(IOBuffer("#12\n123\n45\n67"), buffersize=4, limit=1, skipto=2, comment=comment)
        @test take!(q).results.newline_positions == Int32[0, 3]
        ChunkedBase.dec!(counter)
        @test counter.n == 0
        @test Base.n_avail(q.queue) == 0

        q, counter = test_serial(IOBuffer("#12\n#23\n45\n67"), buffersize=4, limit=1, skipto=2, comment=comment)
        @test take!(q).results.newline_positions == Int32[0, 3]
        ChunkedBase.dec!(counter)
        @test counter.n == 0
        @test Base.n_avail(q.queue) == 0

        q, counter = test_parallel(IOBuffer("#12\n#23\n45\n67"), buffersize=4, limit=1, skipto=2, comment=comment)
        @test take!(q).results.newline_positions == Int32[0, 3]
        ChunkedBase.dec!(counter)
        @test counter.n == 0
        @test Base.n_avail(q.queue) == 0

        q, counter = test_serial(IOBuffer("#12\n#23\n#5\n67"), buffersize=4, limit=1, skipto=2, comment=comment)
        @test take!(q).results.newline_positions == Int32[0, 3]
        ChunkedBase.dec!(counter)
        @test counter.n == 0
        @test Base.n_avail(q.queue) == 0

        q, counter = test_parallel(IOBuffer("#12\n#23\n#5\n67"), buffersize=4, limit=1, skipto=2, comment=comment)
        @test take!(q).results.newline_positions == Int32[0, 3]
        ChunkedBase.dec!(counter)
        @test counter.n == 0
        @test Base.n_avail(q.queue) == 0
    end

    q, counter = test_serial(IOBuffer("123\n45\n67\n123\n45\n67"), buffersize=4, skipto=5)
    @test take!(q).results.newline_positions == Int32[0, 3]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    q, counter = test_parallel(IOBuffer("123\n45\n67\n123\n45\n67"), buffersize=4, skipto=5)
    @test take!(q).results.newline_positions == Int32[0, 3]
    ChunkedBase.dec!(counter)
    @test counter.n == 0
    @test Base.n_avail(q.queue) == 0

    @test_throws ChunkedBase.NoValidRowsInBufferError test_serial(IOBuffer("12345"), buffersize=4)
    @test_throws ChunkedBase.NoValidRowsInBufferError test_parallel(IOBuffer("12345"), buffersize=4)
    @test_throws ChunkedBase.UnmatchedQuoteError test_serial(IOBuffer("\"ab"), buffersize=4, lexer_args=('\\', '"', '"'))
    @test_throws ChunkedBase.UnmatchedQuoteError test_parallel(IOBuffer("\"ab"), buffersize=4, lexer_args=('\\', '"', '"'))
end

@testset "MmapStream" begin
    mktemp() do path, io
        write(io, "123\n45\n67\n123\n45\n67")
        flush(io)
        seekstart(io)
        q, counter = test_parallel(ChunkedBase.MmapStream(io), buffersize=4, skipto=5)
        @test take!(q).results.newline_positions == Int32[0, 3]
        ChunkedBase.dec!(counter)
        @test counter.n == 0
        @test Base.n_avail(q.queue) == 0
    end

    mktemp() do path, io
        compr_io = GzipCompressorStream(io)
        write(compr_io, "123\n45\n67\n123\n45\n67")
        close(compr_io)
        q, counter = test_parallel(GzipDecompressorStream(ChunkedBase.MmapStream(open(path, "r"))), buffersize=4, skipto=5)
        @test take!(q).results.newline_positions == Int32[0, 3]
        ChunkedBase.dec!(counter)
        @test counter.n == 0
        @test Base.n_avail(q.queue) == 0
    end

    mktemp() do path, io
        write(io, "123\n")
        flush(io)
        seekstart(io)
        mm = ChunkedBase.MmapStream(io)
        @test read(mm, UInt8) == UInt8('1')
        @test read(mm, UInt8) == UInt8('2')
        @test filesize(mm) == 4
        @test isopen(mm)
        @test !eof(mm)
        @test read(mm, UInt8) == UInt8('3')
        @test read(mm, UInt8) == UInt8('\n')
        @test eof(mm)
        @test isopen(mm.ios)
        @test !isopen(mm)
        close(mm)
        @test !isopen(mm.ios)
    end
end

@testset "Exception handling" begin
    @testset "consume!" begin
        @testset "serial" begin
            throw_ctx = TestThrowingContext(2)
            @test_throws ErrorException("These contexts are for throwing, and that's all what they do") begin
                parse_file_serial(
                    Lexer(IOBuffer("[1,2]\n[3,4]"), nothing, '\n'),
                    TestParsingContext(),
                    throw_ctx,
                    ChunkingContext(6, 1, 0, nothing),
                    make_buf(1),
                )
            end
            @assert !isempty(throw_ctx.tasks)
            @test throw_ctx.tasks[1] === current_task()
            @test throw_ctx.conds[1].exception isa ErrorException
        end

        @testset "parallel" begin
            # 1500 rows should be enough to get each of the 3 task at least one consume!
            throw_ctx = TestThrowingContext(1500)
            nworkers = min(3, Threads.nthreads())
            @test_throws TaskFailedException begin
                parse_file_parallel(
                    Lexer(IOBuffer(("[1,2]\n[3,4]\n" ^ 800)), nothing, '\n'), # 1600 rows total
                    TestParsingContext(),
                    throw_ctx,
                    ChunkingContext(12, nworkers, 0, nothing),
                    [make_buf(1) for _ in 1:(2*nworkers)],
                )
            end
            sleep(0.2)
            @test length(throw_ctx.tasks) == nworkers
            @test all(istaskdone, throw_ctx.tasks)
            @test throw_ctx.conds[1].exception isa CapturedException
            @test throw_ctx.conds[1].exception.ex.msg == "These contexts are for throwing, and that's all what they do"
        end
    end

    @testset "io" begin
        @testset "serial" begin
            throw_ctx = TestThrowingContext(typemax(Int)) # Only capture tasks, let IO do the throwing
            @test_throws ErrorException("That should be enough data for everyone") begin
                parse_file_serial(
                    Lexer(ThrowingIO(("[1,2]\n[3,4]\n" ^ 10)), nothing, '\n'), # 20 rows total
                    TestParsingContext(),
                    throw_ctx,
                    ChunkingContext(6, 1, 0, nothing),
                    make_buf(1),
                )
            end
            @assert !isempty(throw_ctx.tasks)
            @test throw_ctx.tasks[1] === current_task()
            @test throw_ctx.conds[1].exception isa ErrorException
        end

        @testset "parallel" begin
            throw_ctx = TestThrowingContext(typemax(Int)) # Only capture tasks, let IO do the throwing
            nworkers = min(3, Threads.nthreads())
            @test_throws TaskFailedException begin
                parse_file_parallel(
                    Lexer(ThrowingIO(("[1,2]\n[3,4]\n" ^ 800)), nothing, '\n'), # 1600 rows total
                    TestParsingContext(),
                    throw_ctx,
                    ChunkingContext(12, nworkers, 0, nothing),
                    [make_buf(1) for _ in 1:(2*nworkers)],
                )
            end
            sleep(0.2)
            @test length(throw_ctx.tasks) == min(3, Threads.nthreads())
            @test all(istaskdone, throw_ctx.tasks)
            @test throw_ctx.conds[1].exception isa CapturedException
            @test throw_ctx.conds[1].exception.ex.task.result.msg == "That should be enough data for everyone"
            @test throw_ctx.conds[2].exception isa CapturedException
            @test throw_ctx.conds[2].exception.ex.task.result.msg == "That should be enough data for everyone"
        end
    end
end

@testset "PayloadOrderer" begin
    q = PayloadOrderer{TestResultBuffer,TestParsingContext}()
    @test isopen(q)
    c =  ChunkingContext(4,1,0,nothing)
    ordered_results = Channel{ParsedPayload{TestResultBuffer,TestParsingContext}}(Inf)
    payloads = ParsedPayload{TestResultBuffer,TestParsingContext}[]

    n = 1
    for _ in 1:1000
        i = rand(1:100)
        push!(payloads, make_payload(c, i, n))
        n += i
    end
    shuffle!(payloads)
    @assert Base.n_avail(q.queue) == 0

    for p in payloads
        put!(q, p)
    end
    t = Threads.@spawn begin
        for p in payloads
            p = take!(q)
            put!(ordered_results, p)
        end
        close(ordered_results)
    end
    res = collect(ordered_results);
    @test length(res) == length(payloads)
    close(q)
    @test !isopen(q)
    @test issorted([p.row_num for p in res])

    queue = Channel{ParsedPayload{TestResultBuffer,TestParsingContext}}(Inf)
    q = PayloadOrderer(queue)
    @test isopen(q)

    queue = Channel{ParsedPayload{TestResultBuffer,TestParsingContext}}(Inf)
    q = PayloadOrderer{TestResultBuffer,TestParsingContext}(queue)
    @test isopen(q)
end
