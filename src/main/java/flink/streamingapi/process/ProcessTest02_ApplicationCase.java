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
 * @ClassName: ProcessTetst02_ApplicationCase
 * @Description: TODO 需求：监控连续十秒内温度上升超过10度，进行报警
 * @Author: MovieBook_xinll
 * @Date: 2021/5/28 15:04
 * @Version: v1.0
 */
public class ProcessTest02_ApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.socketTextStream("test01", 7777);
        SingleOutputStreamOperator<SensorReader> sensorStream = streamSource.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });

        sensorStream.keyBy("id")
                .process(new MyTempConRisesWarning(10))
                .print();

        env.execute();
    }

    public static class MyTempConRisesWarning extends KeyedProcessFunction<Tuple, SensorReader, String> {
        private ValueState<Double> tempValueState; // 声明一个私有的温度状态
        private ValueState<Long> timeValueState;  // 声明一个私有的时间状态
        private Integer currentTime; // 当前统计的时间间隔

        /**
         * @Description: open方法是: 全生命周期，实例化tempValueState、timeValueState字段
         * @param: [parameters]
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/28 16:09
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            tempValueState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("ProcessTetst02-tempValueState", Double.class, Double.MIN_NORMAL));
            timeValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ProcessTetst02-timeValueState", Long.class));
        }

        public MyTempConRisesWarning(Integer currentTime) {
            this.currentTime = currentTime;
        }

        /**
         * @Description: processElement方法是: 根据逻辑设置注册时间监控器
         * @param: [sensorReader, context, out]
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/28 16:10
         */
        @Override
        public void processElement(SensorReader sensorReader, Context context, Collector<String> out) throws Exception {
            // 取出状态
            Double lastTemp = tempValueState.value();
            Long timerTs = timeValueState.value();

            // 当遇到温度上涨且时间报警监控设置为空，注册一个10秒的时间监控器
            if (lastTemp < sensorReader.getTemperature() && timerTs == null) {
                long ct = context.timerService().currentProcessingTime() + (currentTime * 1000L);  // 注册时间监控：当前执行时间+设置的监控窗口时间为告警的时间闹钟
                context.timerService().registerProcessingTimeTimer(ct);  //注册
                timeValueState.update(ct); // 更新时间状态数据
            }
            // 当遇到温度小于状态监控温度值时，清空时间监控器
            else if (lastTemp > sensorReader.getTemperature() && timerTs != null) {
                context.timerService().deleteProcessingTimeTimer(timerTs);
                timeValueState.clear();  // 清空时间状态
            }

            // 更新温度状态数据
            tempValueState.update(sensorReader.getTemperature());

        }

        /**
         * @Description: onTimer方法是: 触发告警的具体执行
         * @param: [timestamp, ctx, out]
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/28 16:11
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // System.out.println("警告⚠：当前10s内出现连续温度上涨，且涨幅超过10度，请及时处理故障！！！");
            out.collect("警告⚠【"+"传感器："+ctx.getCurrentKey().getField(0)+"当前连续"+currentTime+"秒内出现连续温度上涨，请及时处理故障！！！】");
            timeValueState.clear();  // 清空时间状态
        }


        @Override
        public void close() throws Exception {
            tempValueState.clear(); // 清空温度状态
        }
    }
}
