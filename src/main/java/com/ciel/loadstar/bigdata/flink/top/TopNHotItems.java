package com.ciel.loadstar.bigdata.flink.top;

import com.ciel.loadstar.bigdata.flink.domain.TopEventEntity;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopNHotItems extends KeyedProcessFunction<Long, TopEventEntity, List<String>> {

    private ListState<TopEventEntity> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<TopEventEntity> itemsStateDesc =
                new ListStateDescriptor<TopEventEntity>("itemState-state", TopEventEntity.class);
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }

    @Override
    public void processElement(TopEventEntity topEventEntity, Context context, Collector<List<String>> collector) throws Exception {
        itemState.add(topEventEntity);
        context.timerService().registerEventTimeTimer(topEventEntity.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<String>> out) throws Exception {
        List<TopEventEntity> allItems = new ArrayList<>();
        for (TopEventEntity item: itemState.get()){
            allItems.add(item);
        }

        itemState.clear();
        allItems.sort(new Comparator<TopEventEntity>() {
            @Override
            public int compare(TopEventEntity o1, TopEventEntity o2) {
                return (int) (o2.getActionTimes() - o1.getActionTimes());
            }
        });

        List<String> ret = new ArrayList<>();
        allItems.forEach(x -> ret.add(String.valueOf(x.getEventName())));
        out.collect(ret);
    }
}
