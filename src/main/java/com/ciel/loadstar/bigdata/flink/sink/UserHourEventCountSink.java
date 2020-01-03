package com.ciel.loadstar.bigdata.flink.sink;

import com.ciel.loadstar.bigdata.flink.config.MySqlUtil;
import com.ciel.loadstar.bigdata.flink.domain.HourEventEntity;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class UserHourEventCountSink extends RichSinkFunction<HourEventEntity> {
    String sql = "insert into user_hour_event(user_id, actionTimes, event_time) values(?, ?, ?);";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(HourEventEntity value, Context context) throws Exception {
        PreparedStatement ps = MySqlUtil.getConnection().prepareStatement(sql);
        ps.setLong(1, value.getUserId());
        ps.setLong(2, value.getActionTimes());
        ps.setTimestamp(3, new Timestamp(value.getWindowEnd()));
        ps.addBatch();
        int[] count = ps.executeBatch();
    }

}
