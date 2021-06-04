package flink.streamingapi.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName: SourceTest03_Kafka
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/19 16:38
 * @Version: v1.0
 */
public class SourceTest03_Kafka {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092");
        properties.setProperty("group.id", "test-topic-[0-3]");


        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer<>("MovieBook_xll_test",new SimpleStringSchema(),properties);

        DataStreamSource dataStreamSource = env.addSource(flinkKafkaConsumer);

        dataStreamSource.print("kafka value is:");

        env.execute();


    }
}
