package com.ciel.loadstar.bigdata.flink.config;

import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EsConfigUtil {
    public final static ElkConfig getConfig(){
        Properties nacosProperties = NacosUtil.getProperties();

        ElkHost elkHost = new ElkHost();
        elkHost.setIp(nacosProperties.getProperty(ConfigConstant.NACOS_ES_HOST));
        elkHost.setSchema(nacosProperties.getProperty(ConfigConstant.NACOS_ES_SCHEMA));
        elkHost.setPort(nacosProperties.getProperty(ConfigConstant.NACOS_ES_PORT));
        elkHost.setUsername(nacosProperties.getProperty(ConfigConstant.NACOS_ES_USERNAME));
        elkHost.setPassword(nacosProperties.getProperty(ConfigConstant.NACOS_ES_PASSWORD));

        List<ElkHost> elkHosts = new ArrayList<>();
        elkHosts.add(elkHost);

        ElkConfig elkConfig = new ElkConfig();
        elkConfig.setHosts(elkHosts);
        elkConfig.setClustername(nacosProperties.getProperty(ConfigConstant.NACOS_ES_CLUSTER));

        return elkConfig;
    }

    public final static ArrayList<HttpHost> getHttpHosts(){
        Properties nacosProperties = NacosUtil.getProperties();

        ArrayList<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(
                new HttpHost(
                        nacosProperties.getProperty(ConfigConstant.NACOS_ES_HOST),
                        Integer.parseInt(nacosProperties.getProperty(ConfigConstant.NACOS_ES_PORT)),
                        nacosProperties.getProperty(ConfigConstant.NACOS_ES_SCHEMA)
                )
        );

        return httpHosts;
    }
}
