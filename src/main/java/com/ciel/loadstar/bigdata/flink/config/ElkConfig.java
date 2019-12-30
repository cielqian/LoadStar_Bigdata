package com.ciel.loadstar.bigdata.flink.config;

import java.util.List;

public class ElkConfig {
    private String clustername;
    private String index;
    private List<ElkHost> hosts;

    public String getClustername() {
        return clustername;
    }

    public void setClustername(String clustername) {
        this.clustername = clustername;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public List<ElkHost> getHosts() {
        return hosts;
    }

    public void setHosts(List<ElkHost> hosts) {
        this.hosts = hosts;
    }
}
