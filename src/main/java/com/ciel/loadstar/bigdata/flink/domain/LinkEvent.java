package com.ciel.loadstar.bigdata.flink.domain;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;

public class LinkEvent implements Serializable {
    String profile;

    String eventType;

    String id;

    Object obj;

    Long ts;

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Object getObj() {
        return obj;
    }

    public void setObj(Object obj) {
        this.obj = obj;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public LinkEvent() {
    }

    public String toJson(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("eventType", eventType);
        jsonObject.put("id", id);
        jsonObject.put("obj", obj);
        jsonObject.put("ts", ts);
        jsonObject.put("profile", profile);

        return jsonObject.toJSONString();
    }
}
