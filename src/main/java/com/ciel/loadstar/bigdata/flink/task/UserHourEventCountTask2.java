package com.ciel.loadstar.bigdata.flink.task;

import com.ciel.loadstar.bigdata.flink.config.KafkaUtil;
import com.ciel.loadstar.bigdata.flink.util.DateUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


public class UserHourEventCountTask2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.registerFunction("DateUtil", new DateUtil());
        tableEnv.connect(
                new Kafka()
                .topic("LinkEvent_Dev")
                .properties(KafkaUtil.getProperties())
                .startFromEarliest()
        )
        .withFormat(
                new Json()
                        .failOnMissingField(true)
                        .deriveSchema()
        )
        .withSchema(
                new Schema()
                .field("id", Types.STRING)
                .field("ts", Types.SQL_TIMESTAMP)
                .rowtime(
                        new Rowtime()
                        .timestampsFromField("ts")
                )
        )
        .inAppendMode()
        .registerTableSource("source");

        Table table = tableEnv.sqlQuery("SELECT DateUtil(ts), count(id) from source group by DateUtil(ts)");
        tableEnv.toRetractStream(table, Row.class)
                .addSink(new PrintSinkFunction<>());


        tableEnv.execute("table api");

    }


}
