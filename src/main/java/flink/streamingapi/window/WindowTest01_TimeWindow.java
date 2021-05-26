package flink.streamingapi.window;

import flink.streamingapi.bean.SensorReader;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WindowTets01_TimeWindow
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/25 17:57
 * @Version: v1.0
 */
public class WindowTest01_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // nc -lk 形式监听某个端口获取流式数据
        DataStreamSource<String> source = env.socketTextStream("test01", 7777);

        DataStream<SensorReader> srDataStream = source.map(new MapFunction<String, SensorReader>() {
            @Override
            public SensorReader map(String s) throws Exception {
                String[] split = s.split(" ");
                return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
            }
        });

        /**
         * @Description: main方法是 增量窗口统计案例
         * @param: [args]
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/26 11:12
         */
        SingleOutputStreamOperator<Integer> resultStream01 = srDataStream.keyBy("id")
                // .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .timeWindow(Time.seconds(5))  //窗口函数设置时间为5秒操作一次
                // .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .aggregate(new AggregateFunction<SensorReader, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReader sensorReader, Integer integer) {
                        return integer + 1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                });
        // resultStream01.print();

        /**
         * @Description: main方法是 全局窗口统计案例
         * @param: [args]
         * @return: Tuple3<String,Long,Integer>  全窗口函数可以返回多元素信息内容
         * @auther: MovieBook_xinll
         * @date: 2021/5/26 11:17
         */
        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> resultStream02 = srDataStream.keyBy("id")
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<SensorReader, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReader> input, Collector<Tuple3<String,Long,Integer>> out) throws Exception {
                        String id = tuple.getField(0);  // 获取SensorReader 的id内容
                        Long windEnd = timeWindow.getEnd();  // 获取每个窗口最后的执行时间
                        int count = IteratorUtils.toList(input.iterator()).size(); // 对窗口时间内的每个key数量进行输出
                        out.collect(new Tuple3<>(id,windEnd,count));
                    }
                });


        resultStream02.print();
        env.execute("Execute TimeWindowDemo ...");
    }
}
