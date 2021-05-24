package flink.streamingapi.transform;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @ClassName: TransFormTest05_connect
 * @Description: 多数据流合并 connect && CoMap/CoFlatMap
 * @Author: MovieBook_xinll
 * @Date: 2021/5/20 17:07
 * @Version: v1.0
 */
public class TransFormTest05_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source01 = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        DataStreamSource<String> source02 = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\SenSor3.md");

        ConnectedStreams<String, String> connectSource = source01.connect(source02);
        /**
         * @Description: 该方法直接使用map操作 拼接两个字符串的内容
         * @param: [args]
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/24 10:34
         */
        DataStream<Object> connectResult01 = connectSource.map(new CoMapFunction<String, String, Object>() {

            @Override
            public Object map1(String s) throws Exception {
                return s;
            }

            @Override
            public Object map2(String s) throws Exception {
                return s;
            }
        });

        connectResult01.print("connectResult01 value is: ");


        DataStream<SensorReader> sensorReaderDS01 = source01.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(new String(split[0]), new Long(split[1]), new Double(split[2]));
        });

        DataStream<SensorReader> sensorReaderDS02 = source02.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(new String(split[0]), new Long(split[1]), new Double(split[2]));
        });
        // sensorReaderDS01.print();

        ConnectedStreams<SensorReader, SensorReader> connectSource02 = sensorReaderDS01.connect(sensorReaderDS02);

        SingleOutputStreamOperator<Tuple2<String, Double>> connectResult02 = connectSource02.map(new CoMapFunction<SensorReader, SensorReader, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map1(SensorReader s) throws Exception {
                return new Tuple2<>(s.getId(), s.getTemperature());
            }

            @Override
            public Tuple2<String, Double> map2(SensorReader s) throws Exception {
                return new Tuple2<>(s.getId(), s.getTemperature());
            }
        });

        connectResult02.print("connectResult02 value is :");




        env.execute();
    }
}
