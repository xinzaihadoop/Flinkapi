package flink.streamingapi.state;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: StateTest2_KeydState
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/27 15:40
 * @Version: v1.0
 */
public class StateTest02_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> textStream = env.socketTextStream("test01", 7777);
        SingleOutputStreamOperator<SensorReader> sensorStream = textStream.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });
        // 定义一个有状态的map操作，统计当前分区的数据个数
        SingleOutputStreamOperator<Integer> resultStream = sensorStream.keyBy("id")
                .map(new MyKeyedStateMapFunction());

        resultStream.print();


        env.execute("Executor KeyedState Demo");
    }

    public static class MyKeyedStateMapFunction extends RichMapFunction<SensorReader, Integer> {
        // TODO Keyed State的使用方法 State主要有三种实现，分别为ValueState、MapState和AppendingState，
        //  AppendingState又可以细分为ListState、ReducingState和AggregatingState。
        private ValueState<Integer> keyCountState;

        // 其他方法测试
        private ListState<String>  keyValuesState;


        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));
            keyValuesState = getRuntimeContext().getListState(new ListStateDescriptor<String>("key-listvalue",String.class));

        }

        @Override
        public Integer map(SensorReader sensorReader) throws Exception {
            // TODO keyValuesState 案例演示
//            List<String> list = new ArrayList<>();
//            for (String value: keyValuesState.get()) {
//                String v = value;
//                list.add(v);
//                // keyValuesState.add(v);
//            }
//            keyValuesState.addAll(list);
            //TODO 清空状态
//            keyValuesState.clear();

            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;
        }
    }
}
