package com.commercial.base;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcExample {
    public static void main(String[] args) {
        // 数据库URL，通常是 jdbc:mysql://<hostname>:<port>/<databaseName>
        String url = "jdbc:mysql://frog-test-rds-us.c9xsjegjiudt.us-east-2.rds.amazonaws.com:3306/growalong_prod";
        // 数据库用户名
        String user = "frog_dev_db";
        // 数据库密码
        String password = "TalkTest666,";

        try {
            // 加载并注册JDBC驱动类
            Class.forName("com.mysql.cj.jdbc.Driver");
            // 建立数据库连接
            Connection conn = DriverManager.getConnection(url, user, password);
            System.out.println("连接成功！");
            // 操作数据库...

            // 关闭连接
            conn.close();
        } catch (ClassNotFoundException e) {
            System.out.println("JDBC驱动未找到！");
        } catch (SQLException e) {
            System.out.println("数据库连接失败！");
        }
    }
}