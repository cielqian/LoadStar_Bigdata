package com.ciel.loadstar.bigdata.flink.domain;

import java.text.SimpleDateFormat;
import java.util.Date;

public class HourEventEntity {
    private Long userId;

    private Long windowEnd;

    private Long actionTimes;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getActionTimes() {
        return actionTimes;
    }

    public void setActionTimes(Long actionTimes) {
        this.actionTimes = actionTimes;
    }

    @Override
    public String toString() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return "userId: " + this.userId
                + ",actionTimes：" + this.actionTimes
                + ",windowEnd：" + formatter.format(new Date(this.windowEnd));
    }
}
