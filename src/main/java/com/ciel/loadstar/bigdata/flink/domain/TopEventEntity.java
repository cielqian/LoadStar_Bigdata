package com.ciel.loadstar.bigdata.flink.domain;

import com.alibaba.fastjson.JSON;

public class TopEventEntity {
    private String eventName;

    private Long actionTimes;

    private long windowEnd;

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public Long getActionTimes() {
        return actionTimes;
    }

    public void setActionTimes(Long actionTimes) {
        this.actionTimes = actionTimes;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
