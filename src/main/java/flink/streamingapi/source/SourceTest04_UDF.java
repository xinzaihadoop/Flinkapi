package flink.streamingapi.source;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @ClassName: SourceTest04_UDF
 * @Description: 自定义数据源案例
 * @Author: MovieBook_xinll
 * @Date: 2021/5/20 10:02
 * @Version: v1.0
 */
public class SourceTest04_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReader> dataStreamSource = env.addSource(new MyDataSourceFunction());

        dataStreamSource.print();

        env.execute();
    }

    public static class MyDataSourceFunction implements SourceFunction<SensorReader> {
        // 创建结束标识
        private boolean running = true;


        @Override
        public void run(SourceContext<SensorReader> sourceContext) throws Exception {
            Random random = new Random();
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 30 + random.nextGaussian() * 60);
            }

            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    //在当前温度基础上随机波动
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    sourceContext.collect(new SensorReader(sensorId, System.currentTimeMillis(), newTemp));
                }
                Thread.sleep(5000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
