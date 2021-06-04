package flink.streamingapi.process;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: ProcessTest01_KeyedProcess
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/28 13:35
 * @Version: v1.0
 */
public class ProcessTest01_KeyedProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // DataStreamSource<String> streamSource = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        DataStreamSource<String> streamSource = env.socketTextStream("test01", 7777);
        SingleOutputStreamOperator<SensorReader> sensorStream = streamSource.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });

        sensorStream.keyBy("id").process(new MyKeyedProcessFunction()).print();


        env.execute();
    }

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, SensorReader, Integer> {
        private ValueState<Long> timeState;
        /**
         * @Description: onTimer方法是: 设定的时间触发器的具体方法内容的实现
         * @param: [timestamp, ctx, out]
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/28 14:24
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            // 简单实现，直接打印
            System.out.println("当前的触发时间戳值为：" + timestamp);

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState",Long.class));
        }

        @Override
        public void processElement(SensorReader sensorReader, Context context, Collector<Integer> out) throws Exception {
            out.collect(sensorReader.getId().length());

            // 获取系统当前时间戳
            context.timestamp();
            context.getCurrentKey();
            // 时间操作
            context.timerService().currentProcessingTime(); // 获取当前的时间
            context.timerService().currentWatermark(); // 获取当前的watermark值
            // 注册闹钟
            context.timerService().registerEventTimeTimer(sensorReader.getTimeStamp() + 10l); // 注册时间时间
            context.timerService().registerProcessingTimeTimer(sensorReader.getTimeStamp() + 10l); // 注册预处理时间
            // 若想删除相对位置的执行时间定时器 该怎么做，例如 删除当前时间4s时的（0-10触发器）
            // 可以利用open方法声明全生命周期的时间变量 用于将当前执行的对应时间保存，当达到执行的时间时，再删除原来执行的时间触发器
            // 可以将注册的时间做状态保留
            timeState.update(sensorReader.getTimeStamp() + 10l);

            // 后续删除就可以用保留的时间状态做删除，而不用写死时间参数了
            context.timerService().deleteProcessingTimeTimer(timeState.value());


            // 普通删除闹钟
            context.timerService().deleteEventTimeTimer(1000L); // 删除事件时间
            context.timerService().deleteProcessingTimeTimer(1000L); // 删除预处理时间




        }
    }
}
