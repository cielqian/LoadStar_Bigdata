package com.ciel.loadstar.bigdata.flink.map;

import com.alibaba.fastjson.JSONObject;
import com.ciel.loadstar.infrastructure.events.link.LinkEvent;
import org.apache.flink.api.common.functions.MapFunction;

public class LinkEventMapFunction implements MapFunction<String, LinkEvent> {
    @Override
    public LinkEvent map(String s) throws Exception {
        try {
            return JSONObject.parseObject(s, LinkEvent.class);
        }
        catch (Exception e){
            System.out.println("error: " + s);
        }
        return null;
    }
}
