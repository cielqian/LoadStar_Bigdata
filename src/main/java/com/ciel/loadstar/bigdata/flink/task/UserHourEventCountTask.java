package com.ciel.loadstar.bigdata.flink.task;

import com.ciel.loadstar.bigdata.flink.agg.CountAgg;
import com.ciel.loadstar.bigdata.flink.config.ConfigConstant;
import com.ciel.loadstar.bigdata.flink.config.KafkaUtil;
import com.ciel.loadstar.bigdata.flink.config.NacosUtil;
import com.ciel.loadstar.bigdata.flink.domain.EventTrack;
import com.ciel.loadstar.bigdata.flink.map.EventTrackMapFunction;
import com.ciel.loadstar.bigdata.flink.sink.UserHourEventCountSink;
import com.ciel.loadstar.bigdata.flink.window.WindowHourEventCountFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 用户每5分钟的事件数
 */
public class UserHourEventCountTask {
    public static void main(String[] args) throws Exception {
        String eventTrackTopic = NacosUtil.getProperty(ConfigConstant.NACOS_TOPIC_EVENT_TRACK);

        Properties kafkaProperties = KafkaUtil.getProperties();

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>(eventTrackTopic, new SimpleStringSchema(), kafkaProperties);
        consumer.setStartFromGroupOffsets();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStreamSource = env.addSource(consumer);

        SingleOutputStreamOperator ws =
                dataStreamSource.map(new EventTrackMapFunction())
                .filter(x -> !"".equals(x.getTag()) && x.getTag() != null)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<EventTrack>() {
                    @Override
                    public long extractAscendingTimestamp(EventTrack eventTrack) {
                        return eventTrack.getEventTime();
                    }
                })
                .keyBy(x -> x.getUserId())
                .timeWindow(Time.minutes(5))
                .aggregate(new CountAgg(), new WindowHourEventCountFunction());
        ws.addSink(new UserHourEventCountSink());
        ws.addSink(new PrintSinkFunction());

        env.execute("UserHourEventCountTask");
    }
}
