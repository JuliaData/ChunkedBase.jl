using Test
using ChunkedBase
using SentinelArrays.BufferedVectors
using NewlineLexers


struct TestResultBuffer <: AbstractResultBuffer end

@testset "API basics" begin
    @testset "defaults" begin
        chunking_ctx = ChunkingContext(1, 1, 0, nothing)
        consume_ctx = ChunkedBase.SkipContext() # uses default methods
        @assert chunking_ctx.counter.n == 0

        ChunkedBase.setup_tasks!(consume_ctx, chunking_ctx, 1)
        @test chunking_ctx.counter.n == 1
        ChunkedBase.task_done!(consume_ctx, chunking_ctx)
        @test chunking_ctx.counter.n == 0
        @test ChunkedBase.sync_tasks(chunking_ctx) === nothing # would hang if ntasks != 0
        @test_throws AssertionError ChunkedBase.task_done!(consume_ctx, chunking_ctx)
        ChunkedBase.setup_tasks!(consume_ctx, chunking_ctx, 2)
        @test chunking_ctx.counter.n == 2
        # We only ever increment when we are done wirh all tasks,
        # so we assert the counter is zero before calling `set!` on it.
        @test_throws AssertionError ChunkedBase.setup_tasks!(consume_ctx, chunking_ctx, 2)
        ChunkedBase.task_done!(consume_ctx, chunking_ctx)
        @test chunking_ctx.counter.n == 1
        ChunkedBase.task_done!(consume_ctx, chunking_ctx)
        @test chunking_ctx.counter.n == 0

        @test_throws ArgumentError ChunkedBase.setup_tasks!(consume_ctx, chunking_ctx, 0)
        @test_throws ArgumentError ChunkedBase.setup_tasks!(consume_ctx, chunking_ctx, -1)
    end
end

@testset "TaskCounter" begin
    @testset "defaults" begin
        counter = ChunkedBase.TaskCounter()
        @assert counter.n == 0
        ChunkedBase.set!(counter, 1)
        @test counter.n == 1
        ChunkedBase.dec!(counter)
        @test counter.n == 0
        @test ChunkedBase.wait(counter) === nothing # would hang if ntasks != 0
        @test_throws AssertionError ChunkedBase.dec!(counter)
        ChunkedBase.set!(counter, 2)
        @test counter.n == 2
        # We only ever increment when we are done wirh all tasks,
        # so we assert the counter is zero before calling `set!` on it.
        @test_throws AssertionError ChunkedBase.set!(counter, 2)
        ChunkedBase.dec!(counter, 2)
        @test counter.n == 0

        counter = ChunkedBase.TaskCounter()
        @test_throws ArgumentError ChunkedBase.set!(counter, 0)
        @test_throws ArgumentError ChunkedBase.set!(counter, -1)
        @test_throws ArgumentError ChunkedBase.dec!(counter, 0)
        @test_throws ArgumentError ChunkedBase.dec!(counter, -1)
    end
end


@testset "handle_file_end" begin
    # Lexer{Nothing,Nothing,Nothing} cannot end on a string
    l = NewlineLexers.Lexer(IOBuffer(), nothing)
    @assert eof(l.io)
    l.done = false
    eols = Int32[0]
    ChunkedBase.handle_file_end!(l, eols, 1)
    @test l.done
    @test eols == Int32[0, 2]

    # Lexer{Q,Q,Q}
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))
    @assert eof(l.io)
    l.done = false
    eols = Int32[0]
    ChunkedBase.handle_file_end!(l, eols, 1)
    @test l.done
    @test eols == Int32[0, 2]

    l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))
    l.prev_in_string = typemax(UInt)
    l.prev_escaped = UInt(1)
    l.done = false
    eols = Int32[0]
    ChunkedBase.handle_file_end!(l, eols, 1)
    @test l.done
    @test eols == Int32[0, 2]

    l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))
    l.prev_in_string = typemax(UInt)
    l.prev_escaped = UInt(0)
    l.done = false
    @assert eof(l.io)
    @test_throws ChunkedBase.UnmatchedQuoteError ChunkedBase.handle_file_end!(l, Int32[], 1)

    l = NewlineLexers.Lexer(IOBuffer(), UInt8('"'), UInt8('"'), UInt8('"'))
    l.prev_in_string = typemin(UInt)
    l.prev_escaped = UInt(1)
    l.done = false
    @assert eof(l.io)
    @test_throws ChunkedBase.UnmatchedQuoteError ChunkedBase.handle_file_end!(l, Int32[], 1)

    # Lexer{E,Q,Q}
    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('"'), UInt8('"'))
    @assert eof(l.io)
    l.done = false
    eols = Int32[0]
    ChunkedBase.handle_file_end!(l, eols, 1)
    @test l.done
    @test eols == Int32[0, 2]

    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('"'), UInt8('"'))
    l.prev_in_string = typemax(UInt)
    l.prev_escaped = UInt(1)
    l.done = false
    @assert eof(l.io)
    @test_throws ChunkedBase.UnmatchedQuoteError ChunkedBase.handle_file_end!(l, Int32[], 1)

    l = NewlineLexers.Lexer(IOBuffer(), UInt8('\\'), UInt8('"'), UInt8('"'))
    l.prev_in_string = typemax(UInt)
    l.prev_escaped = UInt(0)
    l.done = false
    @assert eof(l.io)
    @test_throws ChunkedBase.UnmatchedQuoteError ChunkedBase.handle_file_end!(l, Int32[], 1)
end

@testset "estimate_task_size" begin
    function _get_ctx(; last_newline_at, newlines_num, buffersize, nworkers)
        eols = zeros(Int32, newlines_num)
        eols[end] = last_newline_at
        return ChunkingContext(1, ChunkedBase.TaskCounter(), BufferedVector(eols, newlines_num), zeros(UInt8, buffersize), nworkers, 0, nothing)
    end
    # Empty input (only 0 as end of line) -> return 1
    ctx = _get_ctx(; last_newline_at=0, newlines_num=1, buffersize=2*16*1024, nworkers=4)
    @test ChunkedBase.estimate_task_size(ctx) == 1

    # Each row is 1 byte, submit everything in a single task
    ctx = _get_ctx(; last_newline_at=100, newlines_num=100, buffersize=100, nworkers=1)
    @test ChunkedBase.estimate_task_size(ctx) == 100

    ctx = _get_ctx(; last_newline_at=100000, newlines_num=100000, buffersize=100000, nworkers=2)
    @test ChunkedBase.estimate_task_size(ctx) == 50000

    ctx = _get_ctx(; last_newline_at=100000, newlines_num=100000, buffersize=100000, nworkers=3)
    @test ChunkedBase.estimate_task_size(ctx) == 33334

    # Each task should be at least 16KiB (ChunkedBase.MIN_TASK_SIZE_IN_BYTES) worht of data to work on
    ctx = _get_ctx(; last_newline_at=100000, newlines_num=100000, buffersize=100000, nworkers=10)
    @test ChunkedBase.estimate_task_size(ctx) == 16*1024

    # 2 is a minimum for a non-empty input
    ctx = _get_ctx(; last_newline_at=3*16*1024, newlines_num=3, buffersize=3*16*1024, nworkers=2)
    @test ChunkedBase.estimate_task_size(ctx) == 2

    ctx = _get_ctx(; last_newline_at=3*16*1024, newlines_num=6, buffersize=3*16*1024, nworkers=2)
    @test ChunkedBase.estimate_task_size(ctx) == 3

    ctx = _get_ctx(; last_newline_at=3*16*1024, newlines_num=12, buffersize=3*16*1024, nworkers=2)
    @test ChunkedBase.estimate_task_size(ctx) == 6

    ctx = _get_ctx(; last_newline_at=2*16*1024, newlines_num=12, buffersize=2*16*1024, nworkers=4)
    @test ChunkedBase.estimate_task_size(ctx) == 6

    ctx = _get_ctx(; last_newline_at=2*16*1024, newlines_num=12, buffersize=2*16*1024, nworkers=1)
    @test ChunkedBase.estimate_task_size(ctx) == 12
end

@testset "initial_read!" begin
    ctx = ChunkingContext(10, 1, 0, nothing)
    @test ChunkedBase.initial_read!(IOBuffer(""), ctx) == 0

    ctx = ChunkingContext(10, 1, 0, nothing)
    @test ChunkedBase.initial_read!(IOBuffer(" "), ctx) == 1

    ctx = ChunkingContext(1, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer(" "), ctx, true) == 0

    ctx = ChunkingContext(1, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("  "), ctx) == 1

    ctx = ChunkingContext(2, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("  "), ctx) == 2

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("  "), ctx) == 2

    ctx = ChunkingContext(1, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("  "), ctx, true) == 0

    ctx = ChunkingContext(2, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("  "), ctx, true) == 0

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("  "), ctx, true) == 0

    ctx = ChunkingContext(10, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer(" 1"), ctx, true) == 1
    @test ctx.bytes[1] == UInt8('1')

    ctx = ChunkingContext(10, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer(" 1"), ctx) == 2
    @test ctx.bytes[1:2] == b" 1"

    ctx = ChunkingContext(1, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer(" 1"), ctx) == 1
    @test ctx.bytes[1] == UInt8(' ')

    ctx = ChunkingContext(1, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer(" 1"), ctx, true) == 1
    @test ctx.bytes[1] == UInt8('1')

    ctx = ChunkingContext(1, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("  1"), ctx) == 1
    @test ctx.bytes[1] == UInt8(' ')

    ctx = ChunkingContext(1, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("  1"), ctx, true) == 1
    @test ctx.bytes[1] == UInt8('1')

    ctx = ChunkingContext(2, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("  1"), ctx) == 2
    @test ctx.bytes[1:2] == b"  "

    ctx = ChunkingContext(2, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("  1"), ctx, true) == 1
    @test ctx.bytes[1] == UInt8('1')

    ctx = ChunkingContext(10, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("12"), ctx) == 2
    @test ctx.bytes[1:2] == b"12"

    ctx = ChunkingContext(10, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("12"), ctx, true) == 2
    @test ctx.bytes[1:2] == b"12"

    ctx = ChunkingContext(1, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("12"), ctx) == 1
    @test ctx.bytes[1] == UInt8('1')

    ctx = ChunkingContext(2, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("12"), ctx) == 2
    @test ctx.bytes[1:2] == b"12"
end

@testset "initial_read! with BOM" begin
    ctx = ChunkingContext(10, 1, 0, nothing)
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf"), ctx) == 0

    ctx = ChunkingContext(10, 1, 0, nothing)
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf "), ctx) == 1

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf "), ctx, true) == 0

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf  "), ctx) == 2
    @test ctx.bytes[1:2] == b"  "

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf  "), ctx, true) == 0

    ctx = ChunkingContext(10, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf 1"), ctx, true) == 1
    @test ctx.bytes[1] == UInt8('1')

    ctx = ChunkingContext(10, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf 1"), ctx) == 2
    @test ctx.bytes[1:2] == b" 1"

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf 1"), ctx) == 2
    @test ctx.bytes[1:2] == b" 1"

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf 1"), ctx, true) == 1
    @test ctx.bytes[1] == UInt8('1')

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf  1"), ctx) == 3
    @test ctx.bytes[1:3] == b"  1"

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf  1"), ctx, true) == 1
    @test ctx.bytes[1] == UInt8('1')

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf  1"), ctx) == 3
    @test ctx.bytes[1:3] == b"  1"

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf  1"), ctx, true) == 1
    @test ctx.bytes[1] == UInt8('1')

    ctx = ChunkingContext(10, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf12"), ctx) == 2
    @test ctx.bytes[1:2] == b"12"

    ctx = ChunkingContext(10, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf12"), ctx, true) == 2
    @test ctx.bytes[1:2] == b"12"

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf12"), ctx) == 2
    @test ctx.bytes[1] == UInt8('1')

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf1234"), ctx) == 3
    @test ctx.bytes[1:3] == b"123"

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf   4"), ctx, true) == 1
    @test ctx.bytes[1] == UInt8('4')

    ctx = ChunkingContext(3, 1, 0, nothing);
    @test ChunkedBase.initial_read!(IOBuffer("\xef\xbb\xbf   456"), ctx, true) == 3
    @test ctx.bytes[1:3] == b"456"
end

@testset "skip_rows_init!" begin
    function test_skip_rows_init(data, buffersize, rows_to_skip, expected_num_skipped, comment=nothing)
        lexer = NewlineLexers.Lexer(IOBuffer(data), nothing, UInt8('\n'))
        ctx = ChunkingContext(buffersize, 1, 0, comment)
        ChunkedBase.read_and_lex!(lexer, ctx, 0)

        @test ChunkedBase.skip_rows_init!(lexer, ctx, rows_to_skip) == expected_num_skipped
        return ctx
    end

    for buffersize in (4, 8)
        ctx = test_skip_rows_init("aaaa", buffersize, 0, 0)
        @test ctx.newline_positions == [0, 5]
        ctx = test_skip_rows_init("aaaa", buffersize, 1, 1)
        @test ctx.newline_positions == [5]
        ctx = test_skip_rows_init("aaaa", buffersize, 2, 1)
        @test ctx.newline_positions == [5]
    end
    for buffersize in (5, 10)
        ctx = test_skip_rows_init("aaaa\n", buffersize, 1, 1)
        @test ctx.newline_positions == [5]
        ctx = test_skip_rows_init("aaaa\n", buffersize, 2, 1)
        @test ctx.newline_positions == [5]
    end

    for buffersize in (9, 10)
        ctx = test_skip_rows_init("aaaa\nbbbb", buffersize, 0, 0)
        @test ctx.newline_positions == [0, 5, 10]
        ctx = test_skip_rows_init("aaaa\nbbbb", buffersize, 1, 1)
        @test ctx.newline_positions == [5, 10]
        ctx = test_skip_rows_init("aaaa\nbbbb", buffersize, 2, 2)
        @test ctx.newline_positions == [10]
    end
    for buffersize in (10, 15)
        ctx = test_skip_rows_init("aaaa\nbbbb\n", buffersize, 0, 0)
        @test ctx.newline_positions == [0, 5, 10]
        ctx = test_skip_rows_init("aaaa\nbbbb\n", buffersize, 1, 1)
        @test ctx.newline_positions == [5, 10]
        ctx = test_skip_rows_init("aaaa\nbbbb\n", buffersize, 2, 2)
        @test ctx.newline_positions == [10]
        ctx = test_skip_rows_init("aaaa\nbbbb\n", buffersize, 3, 2)
        @test ctx.newline_positions == [10]
    end
end


@testset "prepare_buffer!" begin
    buf = zeros(UInt8, 10)
    io = IOBuffer("xxx12")
    skip(io, 3)
    @test ChunkedBase.prepare_buffer!(io, buf, 10) == 2
    @test buf[1] == UInt8('1')
    @test buf[2] == UInt8('2')

    buf = zeros(UInt8, 10)
    buf[9] = 0x09
    buf[10] = 0x0a
    io = IOBuffer("xxx12")
    skip(io, 3)
    @test ChunkedBase.prepare_buffer!(io, buf, 8) == 2
    @test buf[1] == 0x09
    @test buf[2] == 0x0a
    @test buf[3] == UInt8('1')
    @test buf[4] == UInt8('2')

    buf = zeros(UInt8, 1)
    io = IOBuffer("xxx12")
    skip(io, 3)
    @test ChunkedBase.prepare_buffer!(io, buf, 0) == 1
    @test buf[1] == 0x31

    buf = zeros(UInt8, 2)
    io = IOBuffer("xxx12")
    skip(io, 3)
    @test ChunkedBase.prepare_buffer!(io, buf, 0) == 2
    @test buf[1] == 0x31
    @test buf[2] == 0x32

    buf = zeros(UInt8, 2)
    io = IOBuffer("xxx123")
    skip(io, 3)
    @test ChunkedBase.prepare_buffer!(io, buf, 0) == 2
    @test buf[1] == 0x31
    @test buf[2] == 0x32
end

@testset "_isemptyrow" begin
    @test ChunkedBase._isemptyrow(0, 1, UInt8[])
    @test ChunkedBase._isemptyrow(1, 2, UInt8[])
    @test !ChunkedBase._isemptyrow(1, 4, UInt8[])
    @test ChunkedBase._isemptyrow(1, 3, UInt8['\n', '\r', '\n'])
    @test !ChunkedBase._isemptyrow(1, 3, UInt8['\n', 'a', '\n'])
end

@testset "initial_lex!" begin
    lexer = NewlineLexers.Lexer(IOBuffer("1"), nothing, UInt8('\n'))
    ctx = ChunkingContext(1, 1, 0, nothing)
    @assert lexer.done == false
    @assert ctx.newline_positions == [0]
    ChunkedBase.initial_lex!(lexer, ctx, 0)
    @test ctx.newline_positions == [0]

    lexer = NewlineLexers.Lexer(IOBuffer("1"), nothing, UInt8('\n'))
    ctx = ChunkingContext(1, 1, 0, nothing)
    @assert lexer.done == false
    @assert ctx.newline_positions == [0]
    seekend(lexer.io)
    ChunkedBase.initial_lex!(lexer, ctx, 1)
    @test ctx.newline_positions == [0, 2]

    lexer = NewlineLexers.Lexer(IOBuffer("1"), nothing, UInt8('\n'))
    ctx = ChunkingContext(10, 1, 0, nothing)
    ctx.bytes .= false
    ctx.bytes[5] = UInt8('\n')
    ChunkedBase.initial_lex!(lexer, ctx, 5)
    @test ctx.newline_positions == [0, 5]

    lexer = NewlineLexers.Lexer(IOBuffer("1"), nothing, UInt8('\n'))
    ctx = ChunkingContext(10, 1, 0, nothing)
    ctx.bytes .= false
    ctx.bytes[5] = UInt8('\n')
    ctx.bytes[9] = UInt8('\n')
    ChunkedBase.initial_lex!(lexer, ctx, 10)
    @test ctx.newline_positions == [0, 5, 9]

    lexer = NewlineLexers.Lexer(IOBuffer("1"), nothing, UInt8('\n'))
    ctx = ChunkingContext(1, 1, 0, nothing)
    ctx.newline_positions.elements[1] = 1
    @test_throws AssertionError ChunkedBase.initial_lex!(lexer, ctx, 0)
    @test_throws AssertionError ChunkedBase.initial_lex!(lexer, ctx, 5)

    lexer = NewlineLexers.Lexer(IOBuffer("1"), nothing, UInt8('\n'))
    lexer.done = true
    ctx = ChunkingContext(1, 1, 0, nothing)
    @test_throws AssertionError ChunkedBase.initial_lex!(lexer, ctx, 0)
    @test_throws AssertionError ChunkedBase.initial_lex!(lexer, ctx, 5)

    lexer = NewlineLexers.Lexer(IOBuffer("1"), nothing, UInt8('\n'))
    ctx = ChunkingContext(10, 1, 0, nothing)
    ctx.bytes .= false
    ctx.bytes[5] = UInt8('\n')
    @test_throws ChunkedBase.NoValidRowsInBufferError ChunkedBase.initial_lex!(lexer, ctx, 4)
end

#=
using Coverage
using ChunkedBase
pkg_path = pkgdir(ChunkedBase);
coverage = process_folder(joinpath(pkg_path, "src"));
open(joinpath(pkg_path, "lcov.info"), "w") do io
    LCOV.write(io, coverage)
end;
covered_lines, total_lines = get_summary(coverage);
println("Coverage: $(round(100 * covered_lines / total_lines, digits=2))%");
run(`find $pkg_path -name "*.cov" -type f -delete`);
=#
