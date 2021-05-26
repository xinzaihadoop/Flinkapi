package flink.streamingapi.window;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: WindowTest02_CountWindow
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/26 9:52
 * @Version: v1.0
 */
public class WindowTest02_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //DataStreamSource<String> source = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");

        DataStreamSource<String> source = env.socketTextStream("test01", 7777);
        DataStream<SensorReader> srDataStream = source.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });

        DataStream<Double> avgResultStream = srDataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new MyAvgFunction());

        avgResultStream.print();
        env.execute("Executor CountWindow Demo ...");
    }

    public static class MyAvgFunction implements AggregateFunction<SensorReader, Tuple2<Double, Integer>, Double> {
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReader sensorReader, Tuple2<Double, Integer> tuple) {
            return new Tuple2<>(tuple.f0 + sensorReader.getTemperature(), tuple.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> tuple) {
            return (tuple.f0 / tuple.f1);
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> tuple, Tuple2<Double, Integer> acc) {
            return new Tuple2<>(tuple.f0 + acc.f0, tuple.f1 + acc.f1);
        }
    }
}
