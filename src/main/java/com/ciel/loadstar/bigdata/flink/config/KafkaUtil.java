package com.ciel.loadstar.bigdata.flink.config;

import java.util.Properties;

public class KafkaUtil {
    public static Properties getProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka.loadstar.top:9093");
        props.put("group.id", "flink-group-master1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //key 序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //value 序列化

        return props;
    }
}
