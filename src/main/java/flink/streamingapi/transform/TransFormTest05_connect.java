package flink.streamingapi.transform;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
public class TransFormTest05_connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStream01 = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        DataStreamSource<String> dataStream02 = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\SenSor3.md");

        DataStream<SensorReader> mapDataStream01 = dataStream01.map(line -> {
            String[] fields = line.split("\t");
            return new SensorReader(new String(fields[0]), new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<SensorReader> mapDataStream02 = dataStream02.map(line -> {
            String[] fields = line.split("\t");
            return new SensorReader(new String(fields[0]), new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Tuple2<String, Double>> mapDataStreams = mapDataStream01.map(new MapFunction<SensorReader, Tuple2<String, Double>>() {

            @Override
            public Tuple2<String, Double> map(SensorReader sensorReader) throws Exception {
                return new Tuple2<>(sensorReader.getId(), sensorReader.getTemperature());
            }
        });


        ConnectedStreams<Tuple2<String, Double>, SensorReader> connectedStreams = mapDataStreams.connect(mapDataStream02);

        SingleOutputStreamOperator<Tuple3<String, Double, String>> resultDataStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReader, Tuple3<String, Double, String>>() {

            @Override
            public Tuple3<String, Double, String> map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                if (stringDoubleTuple2.f1 > 30) {
                    return new Tuple3<String, Double, String>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "this is high temperature!!");
                }
                return new Tuple3<String, Double, String>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "this is low temperature!!");
            }

            @Override
            public Tuple3<String, Double, String> map2(SensorReader sensorReader) throws Exception {
                if (sensorReader.getTemperature() > 30) {
                    return new Tuple3<String, Double, String>(sensorReader.getId(), sensorReader.getTemperature(), "this is high temperature!!");
                }
                return new Tuple3<String, Double, String>(sensorReader.getId(), sensorReader.getTemperature(), "this is low temperature!!");
            }
        });

        resultDataStream.print();

        env.execute();
    }
}
