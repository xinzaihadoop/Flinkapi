package com.mb.test;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: FinkSourceTest
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/26 10:29
 * @Version: v1.0
 */
public class FinkSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("test01", 7777);
        source.print();

        env.execute("SourceDemo executor!");
    }
}
