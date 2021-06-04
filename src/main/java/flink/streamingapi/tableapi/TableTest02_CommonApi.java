package flink.streamingapi.tableapi;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest02_CommonApi
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/6/2 15:05
 * @Version: v1.0
 */
public class TableTest02_CommonApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

       /* // 1.1 基于老版本的planner的流处理 useOldPlanner
        EnvironmentSettings oldStreamSetting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSetting);

        // 1.2 基于老版本的planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBathTableEnv = BatchTableEnvironment.create(batchEnv);

        // 1.3 基于Blink的流处理  useBlinkPlanner
        EnvironmentSettings blinkStreamSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSetting);

        // 1.4 基于Blink的批处理
        EnvironmentSettings blinkBatchSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSetting);  // 此处不需要传env参数  因为本身就是流数据*/


        // 2 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String filePath = "D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor4.csv";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("times", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .inAppendMode()
                //.inUpsertMode()
                //.inRetractMode()
                .createTemporaryTable("sensor_Table");  // 注册catalog 临时表
//        Table sensor_table = tableEnv.from("sensor_Table");  // 获取数据生成实体表
        String sql = "select * from sensor_Table";
        Table sensor_table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(sensor_table, Row.class).print();
        env.execute();
    }
}
