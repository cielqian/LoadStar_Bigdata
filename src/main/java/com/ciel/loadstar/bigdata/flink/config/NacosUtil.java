package com.ciel.loadstar.bigdata.flink.config;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class NacosUtil {

    private final static String NAMESPACE = "fc793924-c27a-4993-bf08-2eea72c93d80";
    //dev
//    private final static String NAMESPACE = "8c077941-0acf-4a58-9b1d-6930673e1f72";
    private static Properties nacosProperties = null;

    public final static Properties getProperties(){
        if (nacosProperties == null){
            nacosProperties = new Properties();
            String serverAddr = "http://loadstar.top:8848";
            String dataId = "flink-service.properties";
            String group = "DEFAULT_GROUP";
            Properties nacosClientProperties = new Properties();
            nacosClientProperties.put("serverAddr", serverAddr);
            nacosClientProperties.put("namespace", NAMESPACE);
            ConfigService configService = null;
            try {
                configService = NacosFactory.createConfigService(nacosClientProperties);
                String propertiesContent = configService.getConfig(dataId, group, 5000);
                InputStream inputStream = new ByteArrayInputStream(propertiesContent.getBytes("UTF-8"));

                nacosProperties.load(inputStream);
            } catch (NacosException | IOException e) {
                e.printStackTrace();
            }
        }
        return nacosProperties;
    }

    public final static String getProperty(String key){
        Object value = getProperties().get(key);
        if (value == null){
            return "";
        }
        return value.toString();
    }

}
