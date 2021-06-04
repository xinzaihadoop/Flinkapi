1、Flink的状态一致性分类：
    AT-MOST-ONCE(最多一次)：
     可能会造成数据丢失
    AT-LEAST-ONCE(至少一次)：
     可能会造成数据重复
    EXACTLY-ONCE(精确一次)：
      生产中主要的用法，确保数据不丢失且不重复

端到端一次性消费：
    ·内部保证：
        checkpoint 把状态存盘，发生故障的时候可以恢复，保证内部状态的一致性
    ·Source端：
        可重设数据的读取位置（可以保存偏移量&重置偏移量）
    ·Sink段：
        从故障恢复时，读取的数不重复写入外部系统 （ 幂等写入&事务写入 ）

事务写入：
    事务：
    应用程序中一系列的严密的操作，所有的操作必须成功完成，否则再每个操作中所作的所有更改都会被取消
    一个事务中的一系列操作要么全部成功，要么全部不做
    构建思想：
    构建的事务对应着checkpoint，等到checkpoint真正的完成的时候，才把所有的对应的结果写入sink系统中
实现方式：
    1.预写日志（write-ahead-log） WAL
    ·把结果数据当成状态保存，然后在收到checkpoint完成的通知时，一次性写入sink系统
    ·简单容易实现，由于数据提前在状态后端中做了缓存，所以无论什么sink系统，都能用这种方式一批搞定
    ·DataStream API提供了一个模板类：GenericWriteAheadSink，来实现这种事务性sink
此方案缺点：增大系统数据延迟
    2.两阶段提交（Two-Phase-Commit）2PC
    ·对于每个checkpoint，sink任务都会启动一个事务，并将接下来所有接受到的数据添加进事务中
    ·然后将这些数据写入外部sink系统，但不提交他们-这时只是“预提交”
    ·当它收到checkpoint完成的通知时，它才正式提交事务，实现结果的真正写入
    ·这种方式真正实现了exactly-once，它需要一个提供事务支持的外部sink系统，Flink提供了TwoPhaseCommitSinkFunction接口

2PC对外部sink系统的要求：
    ·外部sink系统必须提供事务支持，或者sink任务必须能够模拟外部系统上的事务
    ·在checkpoint的时间间隔期内，必须能够开启一个事务并接受数据的写入
    ·在收到checkpoint完成的通知之前，事务必须是”等待提交状态“。在故障恢复的情况下，这可能需要一些时间，如果这个时候sink系统关闭事务（例如超时）
     那么未提交的数据就会丢失
    ·sink任务必须能够在进程失败后恢复事务
    ·提交事务必须是幂等操作


两阶段提交2PC流程介绍：
    ·JobManager协调各个TaskManager进行Checkpoint
    ·checkpoint保存在StateBackend中，默认StateBackend是内存级别，可以改为文件级进行持久化
    ·每个内部的transform任务遇到barrier时，都会把状态保存到checkpoint里面
    ·sink任务时首先会把数据写入到外部kafka中，这些数据属于预提交（per-commit）的事务；遇到barrier时，回把状态保存到状态后端，并开启新的预提交事务
    ·当所有算子任务的快照完成，也就是这次的checkpoint完成时，JobManager会向所有的任务发通知，确认这次checkpoint完成
    ·sink任务收到确认通知，正式提交之前的事务，kafka中未确认数据改为“已确认”
    kafka里面要设置隔离级别，设置成read-commit