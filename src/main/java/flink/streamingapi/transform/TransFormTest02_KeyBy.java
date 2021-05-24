package flink.streamingapi.transform;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: KeyBy
 * @Description: keyby算子
 * @Author: MovieBook_xinll
 * @Date: 2021/5/20 15:13
 * @Version: v1.0
 */
public class TransFormTest02_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");

       /* DataStream<SensorReader> mapDataStream = streamSource.map(new MapFunction<String, SensorReader>() {
            @Override
            public SensorReader map(String s) throws Exception {
                String[] split = s.split("\t");
                return new SensorReader(new String(split[0]), new Long(split[1]), new Double(split[2]));
            }
        });*/

        // 上述引入拉姆达表达式写法如下
        DataStream<SensorReader> mapDataStream = streamSource.map(line -> {
            String[] fields = line.split(" ");
            return new SensorReader(new String(fields[0]), new Long(fields[1]), new Double(fields[2]));
        });

        // 两者实现的结果相同，实现上略有差异
        KeyedStream<SensorReader, Tuple> keyedStream = mapDataStream.keyBy("id");
        KeyedStream<SensorReader, String> keyedStream1 = mapDataStream.keyBy(data -> data.getId());
        KeyedStream<SensorReader, String> keyedStream2 = mapDataStream.keyBy(SensorReader::getId);

        // 按照温度字段选取最大值
        SingleOutputStreamOperator<SensorReader> max = keyedStream1.max("temperature");
        // SingleOutputStreamOperator<SensorReader> maxBy = keyedStream1.maxBy("temperature");
        max.print();
        env.execute();
    }
}
