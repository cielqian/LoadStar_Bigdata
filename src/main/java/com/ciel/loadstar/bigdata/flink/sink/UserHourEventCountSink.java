package com.ciel.loadstar.bigdata.flink.sink;

import com.ciel.loadstar.bigdata.flink.domain.HourEventEntity;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class UserHourEventCountSink extends RichSinkFunction<HourEventEntity> {
    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "insert into user_hour_event(user_id, actionTimes, event_time) values(?, ?, ?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(HourEventEntity value, Context context) throws Exception {
        ps.setLong(1, value.getUserId());
        ps.setLong(2, value.getActionTimes());
        ps.setTimestamp(3, new Timestamp(value.getWindowEnd()));
        ps.addBatch();
        int[] count = ps.executeBatch();
    }

    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl("jdbc:mysql://loadstar.top:3306/LoadStar_Metric?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&autoReconnectForPools=true");
        dataSource.setUsername("root");
        dataSource.setPassword("loadstar123!");
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}
