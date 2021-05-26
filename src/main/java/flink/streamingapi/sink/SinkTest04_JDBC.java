package flink.streamingapi.sink;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName: SinkTest04_JDBC
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/24 18:55
 * @Version: v1.0
 */
public class SinkTest04_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        DataStream<SensorReader> srDataStream = source.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });

        srDataStream.addSink(new MyJdbcSinkFunction());

        env.execute();
    }

    public static class MyJdbcSinkFunction extends RichSinkFunction<SensorReader> {
        Connection connection = null;
        PreparedStatement insertStmt = null;   // 声明预处理insert_sql
        PreparedStatement updateStmt = null;   // 声明预处理update_sql
        static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        static final String DB_URL = "jdbc:mysql://10.65.55.30:3306/xll_test?useUnicode=true&characterEncoding=utf8";
        static final String USER = "root";
        static final String PASS = "Yp@Mb#691";

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("获取连接。。。。。。");
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(DB_URL,USER,PASS);
            System.out.println("初始化执行环境。。。。。");
            insertStmt = connection.prepareStatement("insert into SensorReader (id,temp,ts) values (?,?,?)");
            updateStmt = connection.prepareStatement("update SensorReader set temp = ? ,ts = ? where id = ?");
        }

        /**
         * @Description: invoke方法是
         * @param: [value, context]
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/25 15:54
         */

        @Override
        public void invoke(SensorReader value, Context context) throws Exception {
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setLong(2, value.getTimeStamp());
            updateStmt.setString(3, value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.setLong(3, value.getTimeStamp());
                insertStmt.execute();
            }
        }


        /**
         * @Description: close方法是 关闭资源
         * @param: []
         * @return: void
         * @auther: MovieBook_xinll
         * @date: 2021/5/25 15:54
         */
        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
            System.out.println("say GoodBy !!!");
        }
    }
}
