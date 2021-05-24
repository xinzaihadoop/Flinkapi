package flink.streamingapi.transform;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: TransFormTest08_Parition
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/24 14:17
 * @Version: v1.0
 */
public class TransFormTest08_Parition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        source.print("source print:");
        DataStream<String> shuffleSource = source.shuffle();

        //重分区shuffle操作 将分区数据打乱重组
        //shuffleSource.print("shuffleSource print:");


        //bykey
        //bykey可以对key值按照其hascode值进行分组
        DataStream<SensorReader> sensorReaderDataStream = source.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });

       // sensorReaderDataStream.keyBy(s -> s.getId()).print("bykey print:");

        // global
        source.global().print("global print ");



        env.execute();

    }
}
