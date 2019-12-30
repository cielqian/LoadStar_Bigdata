package com.ciel.loadstar.bigdata.flink.map;

import com.ciel.loadstar.bigdata.flink.domain.EventTrack;
import org.apache.flink.api.common.functions.MapFunction;

public class EventTrackMapFunction implements MapFunction<String, EventTrack> {
    @Override
    public EventTrack map(String s) throws Exception {
        String[] xs = s.split(",");
        EventTrack eventTrack = new EventTrack();
        try{
            eventTrack.setEventId(xs[0]);
            eventTrack.setEventTime(Long.parseLong(xs[1]));
            eventTrack.setUserId(Long.parseLong(xs[2]));
            eventTrack.setProfile(xs[3]);
            eventTrack.setEventType(xs[4]);
            eventTrack.setTag(xs[5]);
        }catch (Exception e){
        }
        return eventTrack;
    }
}
