package flink.streamingapi.window;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: WindowTest03_Watermaks
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/26 17:27
 * @Version: v1.0
 */
public class WindowTest03_Watermarks {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //使用EvenTime 设置时间事件语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);
        DataStreamSource<String> source = env.socketTextStream("test01", 7777);
        DataStream<SensorReader> srDataStream = source.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });

        /**
         * @Description: main方法是  第一种实现 乱序数据设置Watermarks水位线
         * @param: srDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor < SensorReader > ( Time.seconds ( 2))
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/26 17:51
         */
        DataStream<Integer> ds1 = srDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReader>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReader sensorReader) {
                return sensorReader.getTimeStamp() * 1000L;
            }
        }).keyBy("id").timeWindow(Time.seconds(5)).aggregate(new AggregateFunction<SensorReader, Integer, Integer>() {
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


        /**
         * @Description: main方法是 乱序数据第二种实现方式，周期性生成Watermarks,所以不需要传入水位延迟设定时间参数
         * @param: srDataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor < SensorReader > ()
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/26 17:52
         */

        SingleOutputStreamOperator<Integer> ds2 = srDataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReader>() {
            @Override
            public long extractAscendingTimestamp(SensorReader sensorReader) {
                return sensorReader.getTimeStamp() * 1000L;
            }
        }).keyBy("id").timeWindow(Time.seconds(5)).aggregate(new AggregateFunction<SensorReader, Integer, Integer>() {
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


        /**
         * @Description: main方法是 实现方式三，有序的Watermarks的设置
         * @param:
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/26 18:17
         */

        SingleOutputStreamOperator<Integer> ds3 = srDataStream.assignTimestamps(new TimestampExtractor<SensorReader>() {
            @Override
            public long extractTimestamp(SensorReader sensorReader, long l) {
                return sensorReader.getTimeStamp();
            }

            @Override
            public long extractWatermark(SensorReader sensorReader, long l) {
                return 2;
            }

            @Override
            public long getCurrentWatermark() {
                return System.currentTimeMillis();
            }
        }).keyBy("id").timeWindow(Time.seconds(10), Time.seconds(2)).aggregate(new AggregateFunction<SensorReader, Integer, Integer>() {
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

        env.execute("executor Watermarks demo");
    }
}
