package flink.streamingapi.process;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: ProcessTest03_SideOutputCase
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/28 16:36
 * @Version: v1.0
 */
public class ProcessTest03_SideOutputCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.socketTextStream("test01", 7777);

        SingleOutputStreamOperator<SensorReader> sensorStream = streamSource.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });
        //定义一个OutputTag，用来表示低温流输出
        OutputTag<SensorReader> lowTempTag = new OutputTag<SensorReader>("low-temp") {
        };
        //自定义测输出流，对温度做高低温区分，此处不需要对key做分组操作
        SingleOutputStreamOperator<SensorReader> resultStream = sensorStream.process(new ProcessFunction<SensorReader, SensorReader>() {
            @Override
            public void processElement(SensorReader sensorReader, Context context, Collector<SensorReader> out) throws Exception {
                if (sensorReader.getTemperature() > 50.0){
                    out.collect(sensorReader);
                }else {
                    // 测输出流通过Context调用output方法获取，传入测输出标签和对应数据即可
                    context.output(lowTempTag,sensorReader);
                }
            }
        });

        resultStream.print("high-temp");
        resultStream.getSideOutput(lowTempTag).print("low-temp!!");
        env.execute();
    }
}
