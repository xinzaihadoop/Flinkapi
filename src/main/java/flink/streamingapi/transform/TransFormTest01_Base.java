package flink.streamingapi.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Base
 * @Description: 基础算子
 * @Author: MovieBook_xinll
 * @Date: 2021/5/20 13:59
 * @Version: v1.0
 */
public class TransFormTest01_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\SenSor.md");
        
        // map操作
        DataStream<Integer> mapDataStream = streamSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });
        
        
        // flatMap操作
        DataStream<String> flatMapDataSource = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> out) throws Exception {
                for (String words : s.split(" ")) {
                    out.collect(words);
                }
            }
        });

        // filter操作
        DataStream<String> filterDataSource = streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("moviebook_xll");
            }
        });

        mapDataStream.print("mapDataStream value is:");
        flatMapDataSource.print("flatMapDataSource value is:");
        filterDataSource.print("filterDataSource value is:");

        env.execute();

    }
}
