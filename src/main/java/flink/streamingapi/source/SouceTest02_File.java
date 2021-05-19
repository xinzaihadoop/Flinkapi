package flink.streamingapi.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: SouceTest02_File
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/19 14:51
 * @Version: v1.0
 */
public class SouceTest02_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        //读取本地文件内容
        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\SenSor.md");

        dataStreamSource.print("dataStreamSource value is:");

        env.execute();
    }
}
