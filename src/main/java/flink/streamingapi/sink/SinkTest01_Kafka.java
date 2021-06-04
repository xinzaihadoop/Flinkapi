package flink.streamingapi.sink;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;


import java.util.Properties;

/**
 * @ClassName: SinkTest01_Kafka
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/24 14:46
 * @Version: v1.0
 */
public class SinkTest01_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        DataStream<SensorReader> sensorReaderDataStream = dataStreamSource.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092");
        properties.setProperty("group.id", "test-topic-[0-3]");

        sensorReaderDataStream.addSink(new FlinkKafkaProducer<SensorReader>("test01:9092", "MovieBook_xll_test", (KeyedSerializationSchema<SensorReader>) new SimpleStringSchema()));
        
        env.execute();
    }
}