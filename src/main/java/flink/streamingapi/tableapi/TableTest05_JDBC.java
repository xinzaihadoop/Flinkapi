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
 * @ClassName: TableTest05_JDBC
 * @Description: TODO 该案例用于测试flink tableapi通过jdbc方式连接操作mysql数据
 * @Author: MovieBook_xinll
 * @Date: 2021/6/3 15:21
 * @Version: v1.0
 */
public class TableTest05_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sinkDDL = "`create` `table` jdbcOutputTable (" +
                "id `varchar(20)` not null," +
                "cnt `bigint` not null," +
                "temp `double` not null" +
                ")  `with` ( " +
                "'connector.type'='jdbc'," +
                "'connector.url'='jdbc:mysql://10.65.55.30:3306/xll_test'," +
                "'connector.driver'='com.mysql.jdbc.Driver'," +
                "'connector.username'='root'," +
                "'connector.table' = 'jdbcOutputTable'," +
                "'connector.password'='Yp@Mb#691')";


        // 从kafka读取数据
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("MovieBook_xll_test")
                .property("zookeeper.connect", "kafka01:8081,kafka02:8081,kafka03:8081")
                .property("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092")
                .property("auto.offset.reset", "latest"))
                .inAppendMode()
                // 更改成更新模式
                //.inUpsertMode()
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("times", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("jdbc_table");

        String sql = "select id,count(times) as ct,sum(temp) as st from jdbc_table group by id";
        // String sql = "select * from jdbc_table";
         Table updateTable = tableEnv.sqlQuery(sql);


        // 文本形式的输入源事append模式的 和此处的
        tableEnv.sqlUpdate(sinkDDL); // 执行表创建

        // TODO 遗留问题：此处连接不上mysql，也没搞懂如何更新mysql 的数据用
        // updateTable.insertInto("jdbcOutputTable"); // 写入mysql中



        env.execute();
    }
}
