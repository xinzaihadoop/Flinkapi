package flink.streamingapi.sink;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @ClassName: SinkTest03_ES
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/24 17:31
 * @Version: v1.0
 */
public class SinkTest03_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> sourceDS = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        SingleOutputStreamOperator<SensorReader> srMapStream = sourceDS.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });

        // 构建连接es执行环境
        List<HttpHost> httpsHost = new ArrayList<>();
        httpsHost.add(new HttpHost("test01",9200));
        DataStreamSink<SensorReader> esSink = srMapStream.addSink(new ElasticsearchSink.Builder<SensorReader>(httpsHost, new MyEsSinkFunction()).build());


        env.execute();
    }
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReader>{
        /**
         * @Description: process方法是 真正实现数据存储的逻辑部分
         * @param: [sensorReader, runtimeContext, requestIndexer]
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/24 18:41
         */
        @Override
        public void process(SensorReader sensorReader, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            //定义写入的数据源
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id",sensorReader.getId());
            dataSource.put("temp",sensorReader.getTemperature().toString());
            dataSource.put("ts",sensorReader.getTimeStamp().toString());

            // 创建请求，作为es发送数据的写入命令
            IndexRequest indexRequest = Requests.indexRequest().index("sensor").type("readingdata").source(dataSource);

            //用index发送请求
            requestIndexer.add(indexRequest);

        }
    }
}
