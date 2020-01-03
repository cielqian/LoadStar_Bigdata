package com.ciel.loadstar.bigdata.flink.config;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlUtil {

    private static ComboPooledDataSource dataSource = null;

    public static DataSource getDataSource() {
        if (dataSource == null){
            dataSource = new ComboPooledDataSource();
            try {
                dataSource.setDriverClass("com.mysql.jdbc.Driver");
                dataSource.setJdbcUrl("jdbc:mysql://loadstar.top:3306/LoadStar_Metric?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&autoReconnectForPools=true");
                dataSource.setUser("root");
                dataSource.setPassword("loadstar123!");
                dataSource.setInitialPoolSize(10);
                dataSource.setIdleConnectionTestPeriod(120);
                dataSource.setMaxPoolSize(50);
                dataSource.setMinPoolSize(2);
            } catch (PropertyVetoException e) {
                e.printStackTrace();
            }
        }

        return dataSource;
    }

    public static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    public static void close(ResultSet resultSet, PreparedStatement pst, Connection connection) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pst != null) {
            try {
                pst.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void execute(PreparedStatement pst) {
        try {
            Connection connection = getConnection();
            pst.execute();
            close(null, pst, connection);
        } catch (SQLException e) {
            System.out.println("异常提醒：" + e);
        }
    }
}
