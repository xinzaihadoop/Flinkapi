package flink.streamingapi.state;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: StateTest03_ApplicationKeyedState
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/27 17:03
 * @Version: v1.0
 */
public class StateTest03_ApplicationKeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //开启事件支持

        DataStreamSource<String> textStream = env.socketTextStream("test01", 7777);
        SingleOutputStreamOperator<SensorReader> sensorStream = textStream.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });
        // 定义一个有状态的map操作，统计当前分区的数据个数
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = sensorStream.keyBy("id").flatMap(new MyKeyedStateFlatMapFunction(10.0));

        resultStream.print();


        env.execute("Executor ApplicationKeyedState Demo");
    }
    public static class MyKeyedStateFlatMapFunction extends RichFlatMapFunction<SensorReader, Tuple3<String,Double,Double>>{
        private Double temperThreshold;  // 声明温度阈值私有属性
        private ValueState<Double> valueState; // 声明一个私有的valueState
        public MyKeyedStateFlatMapFunction(Double temperThreshold) {
            this.temperThreshold = temperThreshold;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("StateTest03_ApplicationKeyedState",Double.class));
        }

        @Override
        public void flatMap(SensorReader value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double tempState = valueState.value();
            if (tempState != null) {
                // 对取到的温度差值取绝对值
                Double tempDiff = Math.abs(value.getTemperature() - tempState);
                if (tempDiff >= temperThreshold ){
                    out.collect(new Tuple3<>(value.getId(),tempState,value.getTemperature()));
                }
            }
            //更新状态内容
            valueState.update(value.getTemperature());
        }

        @Override
        public void close() throws Exception {
           valueState.clear();
        }
    }
}
