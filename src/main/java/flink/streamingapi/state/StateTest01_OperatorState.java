package flink.streamingapi.state;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @ClassName: StateTest_OperatorState
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/27 14:14
 * @Version: v1.0
 */
public class StateTest01_OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> textStream = env.socketTextStream("test01", 7777);
        SingleOutputStreamOperator<SensorReader> sensorStream = textStream.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });
        // 定义一个有状态的map操作，统计当前分区的数据个数
        SingleOutputStreamOperator<Integer> resultStream = sensorStream.map(new MyStateMapFunction());
        resultStream.print();


        env.execute("Executor OperatorState Demo");
    }
//TODO OperatorState 实现方法 通过继承ListCheckpointed接口，实现snapshotState和restoreState方法
    public static class MyStateMapFunction implements MapFunction<SensorReader, Integer>, ListCheckpointed<Integer> {
        // 定义一个本地变量，作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReader sensorReader) throws Exception {
            count++;
            return count;
        }

        /**
         * @Description: snapshotState方法是: 用来设置快照信息
         * @param: [l, l1]
         * @return: java.util.List<java.lang.Integer>
         * @auther: MovieBook_xinll
         * @date: 2021/5/27 14:38
         */
        @Override
        public List<Integer> snapshotState(long l, long l1) throws Exception {
            return Collections.singletonList(count);
        }

        /**
         * @Description: restoreState方法是: 当作业失败恢复后从快照中获取变量值
         * @param: [list]
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/27 14:39
         */
        @Override
        public void restoreState(List<Integer> list) throws Exception {
            for (Integer nums : list) {
                count += nums;
            }
        }
    }
}
