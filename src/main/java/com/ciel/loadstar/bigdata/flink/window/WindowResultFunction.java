package com.ciel.loadstar.bigdata.flink.window;

import com.ciel.loadstar.bigdata.flink.domain.TopEventEntity;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultFunction
    implements WindowFunction<Long, TopEventEntity, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Long> aggregateResult, Collector<TopEventEntity> collector) throws Exception {
        String event = key;
        Long count = aggregateResult.iterator().next();
        TopEventEntity topEventEntity = new TopEventEntity();
        topEventEntity.setEventName(event);
        topEventEntity.setWindowEnd(timeWindow.getEnd());
        topEventEntity.setActionTimes(count);
        collector.collect(topEventEntity);
    }
}
