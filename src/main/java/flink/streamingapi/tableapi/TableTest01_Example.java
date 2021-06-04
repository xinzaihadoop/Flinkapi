package flink.streamingapi.tableapi;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableAPITest01_Example
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/6/2 13:23
 * @Version: v1.0
 */
public class TableTest01_Example {
    public static void main(String[] args) throws Exception {
        // TODO 该案例本质还是将Stream流转换成Table流之后再转换回Stream流进行执行。
        // TODO 这种方式没有将Stream API和Table API完全脱离 适用该场景，到没做到两者的脱离

        // 创建stream执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        // 创建object
        DataStream<SensorReader> dataStream = streamSource.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });

        // 创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //基于流API创建一个tableAPI
        Table dataTable = tableEnv.fromDataStream(dataStream);
        // 调用table api 进行转换操作 得到新的table api
        Table resultTable = dataTable.select("id,temperature").where("id  = 'sensor_1'");

        // 执行sql
        tableEnv.createTemporaryView("sensor",dataTable);


        String sql = "select * from sensor where id = 'sensor_1'";
        String sql2 = "select count(id) from sensor where id = 'sensor_1'";


        Table resultSqlTable = tableEnv.sqlQuery(sql2);

        //执行打印操作，可以将Table类型再转换回Stream类型 再进行打印
        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable: "); //Row.class 代表获取当前表左右字段类型
        // tableEnv.toAppendStream(resultSqlTable,Row.class).print("resultSqlTable: ");

        tableEnv.toRetractStream(resultSqlTable,Row.class).print("resultSqlTable:");

        env.execute();
    }
}
