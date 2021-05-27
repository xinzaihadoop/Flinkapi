时间语义：
    ·如何设置时间语义 Event time
    ·水位线watermark
    ·水位线的传递、引入和设定

Flink三种不同的时间语义
    ·Event Time： 事件创建时间
    ·Ingestion Time：数据进入Flink的时间
    ·Processing Time：执行操作算子的本地系统时间，与机器相关
    若在代码中不进行设置的话。系统默认（Processing Time） 系统处理时间语义

Flink-Watermark：
 介绍：在使用 EventTime 处理 Stream 数据的时候会遇到数据乱序的问题，流处理从 Event(事 件)产生，流经 Source，
     再到 Operator，这中间需要一定的时间。虽然大部分情况下，传输到 Operator 的数据都是按照事件产生的时间顺序来的，
     但是也不排除由于网络延迟等原因而导致乱序的产生，特别是使用 Kafka 的时候，多个分区之间的数据无法保证有序。
     因此， 在进行 Window 计算的时候，不能无限期地等下去，必须要有个机制来保证在特定的时间后， 必须触发 Window
     进行计算，这个特别的机制就是 Watermark(水位线)。Watermark 是用于 处理乱序事件的。
    ·Watermark是一条特殊的记录
    ·Watermark必须单调递增，以确保任务的事件时间是在向前推进而不是在往后退
    ·Watermark与数据的时间戳有关

实现方式：
第一种实现 乱序数据设置Watermarks水位线
    srDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor < SensorReader > ( Time.seconds ( 2))
第二种实现，周期性生成Watermarks,所以不需要传入水位延迟设定时间参数
    srDataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor < SensorReader > ()
第三种实现，数据有序的Watermarks的设置
    srDataStream.assignTimestamps(new TimestampExtractor<SensorReader>()
         
若是设置了Watermarks还是存在数据乱序的情况，则可以手动设置数据延迟，再不行就设置
    srDataStream.TimeWindow(Time.Seconds(15)).allowedLateness(Time.minutes(1))
此处的设定意思是当Watermarks不能满足系统处理乱序的情况时，手动设置一分钟等待时间，让窗口晚一分钟关闭执行
若是还不想将后续的垃圾（延迟）数据丢掉，则可以设置侧输出流，让其得到计算处理
srDataStream.TimeWindow(Time.Seconds(15)).allowedLateness(Time.minutes(1)).sideOutputLateData("测输出流标签")

