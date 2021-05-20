package flink.streamingapi.transform;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: TransFormTest03_Reduce
 * @Description: Reduce聚合算子
 * @Author: MovieBook_xinll
 * @Date: 2021/5/20 15:56
 * @Version: v1.0
 */
public class TransFormTest03_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        DataStream<SensorReader> mapDataStream = streamSource.map(line -> {
            String[] fields = line.split("\t");
            return new SensorReader(new String(fields[0]), new Long(fields[1]), new Double(fields[2]));
        });
        KeyedStream<SensorReader, Tuple> keyedStream = mapDataStream.keyBy("id");

        /**
         * @Description: main方法是 用于输出同一个keyid下的最大温度和最新时间
         * @param: [args]
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/20 16:08
         */
        SingleOutputStreamOperator<SensorReader>  resultDataStream1 = keyedStream.reduce(new ReduceFunction<SensorReader>() {
            @Override
            public SensorReader reduce(SensorReader sr1, SensorReader sr2) throws Exception {
                return new SensorReader(sr1.getId(), sr2.getTimeStamp(), Math.max(sr1.getTemperature(), sr2.getTemperature()));
            }
        });

        // 方式二 采用拉姆达表达式
        ReduceFunction<SensorReader> resultDataStream2 = (sr1, sr2) -> {
            SensorReader sensorReader = new SensorReader(sr1.getId(), sr2.getTimeStamp(), Math.max(sr1.getTemperature(), sr2.getTemperature()));
            return sensorReader;
        };

        resultDataStream1.print();

        env.execute();
    }
}
