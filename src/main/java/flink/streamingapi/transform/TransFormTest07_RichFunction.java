package flink.streamingapi.transform;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: TransFormTest07_RichFunction
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/24 13:31
 * @Version: v1.0
 */
public class TransFormTest07_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        DataStream<SensorReader> srDataStream = source.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });

        DataStream<Tuple2<String, Long>> tuple2DataStream = srDataStream.map(new Tup2Map());
        tuple2DataStream.print();

        env.execute();
    }

    /**
     * 普通的mapFunction中只能实现主方法以及java对应的object方法
     * 没法实现扩展功能的方法
     */
    public static class Tup2Map implements MapFunction<SensorReader, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> map(SensorReader s) throws Exception {
            return new Tuple2<>(s.getId(), new Long(s.getId().length()));
        }
    }

    /**
     * RichFunction:实现基本功能的同事还可以查看执行状态等其他状态信息
     */
    public static class Tup2RichMapFunction extends RichMapFunction<SensorReader, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReader s) throws Exception {
            // getRuntimeContext() 获取执行环境信息
            return new Tuple2<>(s.getId(), new Integer(String.valueOf(getRuntimeContext())));
        }

        /**
         * @Description: open方法是 一般用作初始化工作，状态定义或者数据库连接工作
         * @param: [parameters]
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/24 14:07
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open");
        }

        /**
         * @Description: close方法是 一般是用作数据库连接状态关闭或者清空状态
         * @param: []
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/24 14:08
         */
        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close");
        }
    }
}
