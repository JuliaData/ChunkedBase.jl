using GLMakie, ChunkedBase

function plot_traces()
    t1 = copy(ChunkedBase.T1) # total parse/consume time for the first byte buffer by all workers
    t2 = copy(ChunkedBase.T2) # total parse/consume time for the second byte buffer by all workers
    io_task = copy(ChunkedBase.IO_TASK_TIMES) # time spent in IO
    lexer_task = copy(ChunkedBase.LEXER_TASK_TIMES) # time spent in lexer
    parser_tasks = filter(x->length(x)>0, ChunkedBase.PARSER_TASKS_TIMES) # individual parse/consume times for each worker
    consume_tasks = filter(x->length(x)>0, ChunkedBase.CONSUMER_TASKS_TIMES) # optional extensions for consumes that spawn tasks

    start = Int(mapreduce(first, min, parser_tasks, init=min(io_task[1], lexer_task[1])))

    # convert to seconds, subtract start time
    lexer_timing = map(x->(x - start) / (1e9), lexer_task)
    io_timing = map(x->(x - start) / (1e9), io_task)
    pa_timings = map.(x->(x - start) / (1e9), parser_tasks)
    co_timings = map.(x->(x - start) / (1e9), consume_tasks)
    t1_timing = map(x->(x - start) / (1e9), t1)
    t2_timing = map(x->(x - start) / (1e9), t2)

    fig = GLMakie.linesegments(io_timing, fill(1, length(io_timing)))
    ends = 2:2:length(io_timing)
    GLMakie.scatter!(io_timing[ends], fill(1, length(ends)))

    for (i, timing) in enumerate(vcat([lexer_timing, t1_timing, t2_timing, Float64[]], pa_timings, [Float64[]], co_timings))
        GLMakie.linesegments!(timing, fill(i+1, length(timing)))
        ends = 2:2:length(timing)
        GLMakie.scatter!(timing[ends], fill(i+1, length(ends)))
    end
    total = Base.Ryu.writefixed((lexer_task[end] - io_task[1]) / 1e9, 3) * "s"
    io_work = Base.Ryu.writefixed(@views(mapreduce((a,b)->(b-a)/1e9, +, io_task[1:2:end-1], io_task[2:2:end])), 3) * "s"
    io_idle = Base.Ryu.writefixed(@views(mapreduce((a,b)->(b-a)/1e9, +, io_task[2:2:end-1], io_task[3:2:end])), 3) * "s"
    lex_work = Base.Ryu.writefixed(@views(mapreduce((a,b)->(b-a)/1e9, +, lexer_task[1:2:end-1], lexer_task[2:2:end])), 3) * "s"
    lex_idle = Base.Ryu.writefixed(@views(mapreduce((a,b)->(b-a)/1e9, +, lexer_task[2:2:end-1], lexer_task[3:2:end])), 3) * "s"
    @info "Timings" total io_work io_idle lex_work lex_idle
    return fig
end
