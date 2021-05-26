package com.mb.test;

import java.sql.*;

/**
 * @ClassName: JdbcMysql
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/25 16:48
 * @Version: v1.0
 */
public class JdbcMysql {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://10.65.55.30:3306/xll_test?useUnicode=true&characterEncoding=utf8";
    static final String USER = "root";
    static final String PASS = "Yp@Mb#691";
    // static final String PASS = "0;4oeErElo7u";
    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(JDBC_DRIVER);

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT * FROM SensorReader";
            ResultSet rs = stmt.executeQuery(sql);
            while(rs.next()){
                // 通过字段检索
                int id  = rs.getInt("id");
                String temp = rs.getString("temp");
                String ts = rs.getString("ts");

                // 输出数据
                System.out.print("ID: " + id);
                System.out.print(", 温度值: " + temp);
                System.out.print(", 时间戳: " + ts);
                System.out.print("\n");
            }
            // 完成后关闭
            rs.close();
            stmt.close();
            conn.close();


        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        System.out.println("say Goodby !!!");
    }
}
