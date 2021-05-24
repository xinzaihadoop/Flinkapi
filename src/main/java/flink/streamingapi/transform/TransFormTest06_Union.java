package flink.streamingapi.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: TransFormTest06_Union
 * @Description:
 * @Author: MovieBook_xinll
 * @Date: 2021/5/24 11:22
 * @Version: v1.0
 */
public class TransFormTest06_Union {

    /* Connect操作可以将不同类型的两个流进行合并操作但是仅限于两个数据流
     * Union可以将多个流数据进行合并操作 但是仅限于同种数据类型之间的操作
     * */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> dataStream01 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> dataStream02 = env.fromElements(6, 7, 8, 9, 10);
        DataStreamSource<String> dataStream03 = env.fromElements("a","b","c","d");
        DataStreamSource<Integer> dataStream04 = env.fromElements(16, 17, 18, 19, 20);
        DataStream<Integer> union = dataStream01.union(dataStream02, dataStream04);
        union.print();
        env.execute();
    }
}
