package flink.streamingapi.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * @ClassName: TableTest03_OutputFile
 * @Description: TODO 将数据输出到文件操作
 * @Author: MovieBook_xinll
 * @Date: 2021/6/3 11:22
 * @Version: v1.0
 */
public class TableTest03_OutputFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String filePath = "D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor4.csv";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("times",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE())
                )
                .inAppendMode()
                .createTemporaryTable("sensor_table");

        Table sensor_table = tableEnv.from("sensor_table");

        Table result = sensor_table.select("id,temp").where("temp > 34.28");

        String outPutPath = "D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\target\\out\\o1.txt";

        // 定义输出路径
        tableEnv.connect(new FileSystem().path(outPutPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp",DataTypes.DOUBLE())
                )
                .inAppendMode()
                .createTemporaryTable("outputTable");  // 注册输出临时表

        // 数据输出
       result.insertInto("outputTable");

       env.execute();


    }
}
