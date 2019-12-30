package com.ciel.loadstar.bigdata.flink.agg;

import com.ciel.loadstar.bigdata.flink.domain.EventTrack;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CountAgg implements AggregateFunction<EventTrack, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(EventTrack eventTrack, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return acc1 + aLong;
    }
}
