package flink.streamingapi.transform;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * @ClassName: TransFormTest04_MultiplStream
 * @Description: 多数据流操作算子 split & select
 * @Author: MovieBook_xinll
 * @Date: 2021/5/20 16:30
 * @Version: v1.0
 */
public class TransFormTest04_MultiplStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        DataStream<SensorReader> mapDataStream = streamSource.map(line -> {
            String[] fields = line.split("\t");
            return new SensorReader(new String(fields[0]), new Long(fields[1]), new Double(fields[2]));
        });

        SplitStream<SensorReader> splitStream = mapDataStream.split(new OutputSelector<SensorReader>() {
            @Override
            public Iterable<String> select(SensorReader sensorReader) {
                // 此处对温度用30度为界限，将一个完整的数据流拆分成不同的两个标签的数据流
                return (sensorReader.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low"));
            }
        });

        splitStream.select("high").print();


        env.execute();
    }
}
