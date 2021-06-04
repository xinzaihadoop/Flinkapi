package flink.streamingapi.state;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: StateTets04_FaultRecovery
 * @Description: TODO 状态后端操作案例
 * @Author: MovieBook_xinll
 * @Date: 2021/5/28 10:50
 * @Version: v1.0
 */
public class StateTets04_FaultRecovery {
    // TDOD 故障容灾备份
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置状态后端
        env.setStateBackend(new MemoryStateBackend());  // 保存之本地内存
        env.setStateBackend(new FsStateBackend("hdfs://test01:9000/xxxxx/xxxx")); // 保存至FSfile文件系统
        env.setStateBackend(new RocksDBStateBackend("hdfs://test01:9000/xxxxx/xxxx"));  //保存进RocksDB


        // 设置检查点
       //  env.enableCheckpointing(); // 不传递参数的话，系统默认500L毫秒时间（该方法以弃用）
        env.enableCheckpointing(300);

        // 检查点的高级用法
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);  // 设置缓存节点的级别 EXACTLY_ONCE：精准一次性
        env.getCheckpointConfig().setCheckpointInterval(300l);                           // 设置检查点间隔300毫秒一次
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);                        // 设置同时可以进行检查点执行的个数
        env.getCheckpointConfig().setCheckpointTimeout(60000l);                          // 设置检查点最大执行时间
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);                  // 设置默认使用Checkpoint进行状态保存
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100l);                   // 设置两个检查点之间执行的最小时间间隔
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);                // 设置检查点允许执行失败次数


        // 重启策略配置

        // 失败率重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.minutes(1))); // 尝试失败次数、重启时间、重启时间间隔

        env.setRestartStrategy(RestartStrategies.fallBackRestart()); // 回滚重启
        // 固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000l)); // 尝试重启次数，两次重启之间的时间间隔


        DataStreamSource<String> textStream = env.socketTextStream("test01", 7777);
        SingleOutputStreamOperator<SensorReader> sensorStream = textStream.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });
        // 定义一个有状态的map操作，统计当前分区的数据个数
        // SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = sensorStream.keyBy("id").flatMap(new StateTest03_ApplicationKeyedState.MyKeyedStateFlatMapFunction(10.0));

        // resultStream.print();
    }
}
