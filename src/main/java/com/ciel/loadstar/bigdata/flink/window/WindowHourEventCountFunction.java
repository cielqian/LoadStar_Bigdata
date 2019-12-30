package com.ciel.loadstar.bigdata.flink.window;

import com.ciel.loadstar.bigdata.flink.domain.HourEventEntity;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowHourEventCountFunction
        implements WindowFunction<Long, HourEventEntity, Long, TimeWindow> {
    @Override
    public void apply(Long key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<HourEventEntity> collector) throws Exception {
        HourEventEntity hourEventEntity = new HourEventEntity();
        hourEventEntity.setUserId(key);
        hourEventEntity.setActionTimes(iterable.iterator().next());
        hourEventEntity.setWindowEnd(timeWindow.getEnd());

        collector.collect(hourEventEntity);
    }
}
