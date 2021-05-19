package flink.streamingapi.source;

import com.sun.xml.internal.bind.v2.TODO;
import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @ClassName: SourceTest01_Collection
 * @Description: Flinksource案例01Collection演示
 * @Author: MovieBook_xinll
 * @Date: 2021/5/19 14:10
 * @Version: v1.0
 */
public class SourceTest01_Collection {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1
        env.setParallelism(1);

        //从集合中读取数据
        DataStreamSource<SensorReader> dataStream = env.fromCollection(Arrays.asList(
                new SensorReader("sensor01", 1621405491L, 15.5),
                new SensorReader("sensor02", 1621319091L, 16.5),
                new SensorReader("sensor03", 1621232691L, 14.5),
                new SensorReader("sensor04", 1621146291L, 17.5),
                new SensorReader("sensor05", 1621059891L, 18.5),
                new SensorReader("sensor06", 1620973491L, 19.5)));

        DataStreamSource<? extends Serializable> dataStream2  = env.fromElements(1, 2,3,4,5);

        dataStream.print("dataStream is :");
        dataStream2.print("dataStream2 is :");

        env.execute();

    }
}
