package com.stream.common.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @ClassName com.zh.utils.MySQLConnectionPool
 * @Author han.zhou
 * @Date 2023/9/14 9:08
 */
public class MySQLConnectionPool {
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;
    private static final String MYSQL_HOST = ConfigUtils.getString("mysql.host");
    private static final String MYSQL_PORT = ConfigUtils.getString("mysql.port");
    private static final String MYSQL_DATABASE = ConfigUtils.getString("mysql.trx.databases");
    private static final String MYSQL_USERNAME = ConfigUtils.getString("mysql.username");
    private static final String MYSQL_PASSWORD = ConfigUtils.getString("mysql.password");

    static {
        config.setJdbcUrl("jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE + "?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8");
        config.setUsername(MYSQL_USERNAME);
        config.setPassword(MYSQL_PASSWORD);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(50);      //设置连接池的最大大小
        config.setMinimumIdle(10);          //设置连接池的最小空闲连接数
        config.setConnectionTimeout(10000); //连接超时时间
        config.setIdleTimeout(300000);      //连接空闲的最长时间
        config.setMaxLifetime(600000);      //连接的最长生命周期
        ds = new HikariDataSource(config);
    }

    private MySQLConnectionPool() {}

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}
