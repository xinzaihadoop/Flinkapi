Flink中的状态：
算子状态：Operatior state
键控状态：Keyed state
状态后端：State Backends

Flink的状态管理机制一般分为两种：
    1、manager state ： Flink底层帮我们已经实现了的状态管理器
    2、row state：不通过Flink底层实现。用户可以自定义实现（比较复杂）
然后manager state中又分为Operatior state 和 Keyed state

Flink中：
    1、状态始终与特定的算子相关联
    2、为了使运行中的Flink了解到算子的状态，需要算子进行预先注册其状态
    总的来说分为两种类型的状态；
        1、算子状态：算子状态的作用范围限定为算子任务
            在底层可以又以下几种数据结构组成：
            ListState：存储列表类型的状态。
            UnionListState：存储列表类型的状态，
                与 ListState 的区别在于：如果并行度发生变化，ListState 会将该算子的所有并发的状态实例进行汇总，
                然后均分给新的 Task；而 UnionListState 只是将所有并发的状态实例汇总起来，具体的划分行为则由用户进行定义。
            BroadcastState：用于广播的算子状态。如果一个算子有多项任务，而它的每项任务状态又都相同，那么这种特殊情况最适合应用广播状态。
        2、键控状态：根据输入数据流中的健（key）来维护和访问
            监控状态的数据结构：
            Value state
            LIst state
            Map state
            Reducing state & Aggregating state

Flink的状态后端：
    状态后端可以选择保存在
    1、MemoryStateBackend： 本地内存 （特点：将检查点和状态都放在本地JVM堆内存中。快速，低延迟，不稳定）
    2、FSStateBackend：  HDFS文件系统  （特点：将checkpoint保存在FSFile文件系统上，将本地状态保存进本地JVM堆内存中）
    3、RocksDBsStateBackend：RocksDB数据库中 （特点将所有状态序列化后保存进数据库）

Flink设置检查点Checkpoint：



Flink设置失败重启策略：



