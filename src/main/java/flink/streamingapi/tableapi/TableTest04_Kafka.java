package flink.streamingapi.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest04_Kafka
 * @Description: TODO 此案例实现从kafka读取数据然后将处理后的数据写入kafak中
 * @Author: MovieBook_xinll
 * @Date: 2021/6/3 13:17
 * @Version: v1.0
 */
public class TableTest04_Kafka {
    public static void main(String[] args) throws Exception {
        // 初始化 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 构建kafka输入环境
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("MovieBook_xll_test")
                .property("zookeeper.connect", "kafka01:8081,kafka02:8081,kafka03:8081")
                .property("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092")
                .property("auto.offset.reset", "latest")
        ).withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("times", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .inAppendMode()
                .createTemporaryTable("inSensor_kafka");

        // 转换擦欧总
        Table resultTable = tableEnv.from("inSensor_kafka").where("id === 'sensor_3'");  // 思考：如何在where后面加入多级条件过滤

        tableEnv.toAppendStream(resultTable, Row.class).print();

        // 构建输出环境
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("MovieBook_xll_test2")
                .property("zookeeper.connect", "kafka01:8081,kafka02:8081,kafka03:8081")
                .property("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092")
        ).withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("times", DataTypes.BIGINT())
                .field("temp", DataTypes.DOUBLE()))
                .inAppendMode()
                .withFormat(new Csv())
                .createTemporaryTable("outSensor_kafka");

        // 将结果数据输出进kafka的另一个topic中
        resultTable.insertInto("outSensor_kafka");

        env.execute();
    }
}
