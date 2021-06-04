package com.mb.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @ClassName: FlinkTableApi_Common
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/6/3 10:31
 * @Version: v1.0
 */
public class FlinkTableApi_Common {
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
                .createTemporaryTable("sensor_Table");

        Table sensor_table = tableEnv.from("sensor_Table");

        tableEnv.toAppendStream(sensor_table, Row.class).print();

        env.execute();

    }
}
