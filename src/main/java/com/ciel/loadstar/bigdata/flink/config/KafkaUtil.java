package com.ciel.loadstar.bigdata.flink.config;

import java.util.Properties;

public class KafkaUtil {
    public static Properties getProperties(){
        Properties nacosProperties = NacosUtil.getProperties();

        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConfigConstant.KAFKA_BOOTSTRAP_SERVERS, nacosProperties.getProperty(ConfigConstant.NACOS_KAFKA_BOOTSTRAP_SERVERS));
        kafkaProperties.put(ConfigConstant.KAFKA_GROUP_ID, nacosProperties.getProperty(ConfigConstant.NACOS_KAFKA_GROUP_ID));
        kafkaProperties.put(ConfigConstant.KAFKA_AUTO_OFFSET_RESET, nacosProperties.getProperty(ConfigConstant.NACOS_KAFKA_AUTO_OFFSET_RESET));
        kafkaProperties.put(ConfigConstant.KAFKA_KEY_DESERIALIZER, nacosProperties.getProperty(ConfigConstant.NACOS_KAFKA_KEY_DESERIALIZER));
        kafkaProperties.put(ConfigConstant.KAFKA_VALUE_DESERIALIZER, nacosProperties.getProperty(ConfigConstant.NACOS_KAFKA_VALUE_DESERIALIZER));

        return kafkaProperties;
    }
}
