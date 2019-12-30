package com.ciel.loadstar.bigdata.flink.map;

import com.ciel.loadstar.bigdata.flink.domain.EventTrack;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class UserEventTrackMapFunction implements MapFunction<EventTrack, Tuple2<Long, Long>> {
    @Override
    public Tuple2<Long, Long> map(EventTrack s) throws Exception {
        return new Tuple2<Long, Long>(s.getUserId(), 1L);
    }
}
