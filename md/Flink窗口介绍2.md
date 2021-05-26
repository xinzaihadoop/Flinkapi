Flink的窗口分为两大类：
    1：时间窗口
        ·滚动时间窗口
        ·滑动时间窗口
        ·会话窗口
    2：计数窗口
        ·滚动计数窗口
        ·滑动计数窗口

Flink windowsAPI 操作案例
   DataStream<Tuple2<String, Integer>> dataStream = env
                   .socketTextStream("localhost", 9999)
                   .flatMap(new Splitter())
                   .keyBy(value -> value.f0)   //在调用window操作之前需要做keyby操作 然后再进行window相应操作
                   .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                   .sum(1);                    //window操作之后需要进行聚合操作


Flink window窗口操作之时间窗口函数
    窗口函数总的来分 分为两大类：
        1：增量聚合函数
            ·每条数据进来就触发计算，保持一个简单的状态
            增量窗口一般在 .window()方法中调用.aggregate()方法
            增量窗口没法拿到其他多余信息（除统计信息之外）
        2：全量窗口函数
            ·先把窗口的数据收集起来，等到计算的时候在遍历全部数据
            全量窗口一般在 .window()方法中调用.apply()方法    
            全量窗口可以拿到其他多余信息（除统计信息之外），尽管效率上不如增量统计，应以使用场景而定