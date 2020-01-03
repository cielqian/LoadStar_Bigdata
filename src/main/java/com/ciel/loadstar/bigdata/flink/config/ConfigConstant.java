package com.ciel.loadstar.bigdata.flink.config;

public class ConfigConstant {
    public final static String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public final static String KAFKA_GROUP_ID = "group.id";
    public final static String KAFKA_AUTO_OFFSET_RESET = "auto.offset.reset";
    public final static String KAFKA_KEY_DESERIALIZER = "key.deserializer";
    public final static String KAFKA_VALUE_DESERIALIZER = "value.deserializer";

    public final static String NACOS_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap-servers";
    public final static String NACOS_KAFKA_GROUP_ID = "kafka.group-id";
    public final static String NACOS_KAFKA_AUTO_OFFSET_RESET = "kafka.auto-offset-reset";
    public final static String NACOS_KAFKA_KEY_DESERIALIZER = "kafka.producer.key-serializer";
    public final static String NACOS_KAFKA_VALUE_DESERIALIZER = "kafka.producer.value-serializer";

    public final static String ES_HOST = "";
    public final static String ES_PORT = "";
    public final static String ES_SCHEMA = "";
    public final static String ES_USERNAME = "";
    public final static String ES_PASSWORD = "";
    public final static String ES_INDEX = "";
    public final static String ES_CLUSTER = "";

    public final static String NACOS_ES_HOST = "es.host";
    public final static String NACOS_ES_PORT = "es.port";
    public final static String NACOS_ES_SCHEMA = "es.schema";
    public final static String NACOS_ES_USERNAME = "es.username";
    public final static String NACOS_ES_PASSWORD = "es.password";
    public final static String NACOS_ES_INDEX = "es.index";
    public final static String NACOS_ES_CLUSTER = "es.clustername";

    public final static String NACOS_TOPIC_LINK_EVENT = "kafka.topic.linkEvent";
    public final static String NACOS_TOPIC_EVENT_TRACK = "kafka.topic.eventTrack";
}
