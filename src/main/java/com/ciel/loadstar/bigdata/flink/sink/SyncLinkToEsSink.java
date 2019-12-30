package com.ciel.loadstar.bigdata.flink.sink;

import com.alibaba.fastjson.JSONObject;
import com.ciel.loadstar.bigdata.flink.domain.ESLink;
import com.ciel.loadstar.bigdata.flink.domain.Link;
import com.ciel.loadstar.bigdata.flink.domain.LinkEvent;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;

import java.util.ArrayList;

public class SyncLinkToEsSink {
    public ElasticsearchSink getSink(){
        ArrayList httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost("loadstar-904239407.us-west-2.bonsaisearch.net", 443, "http"));
        ElasticsearchSink.Builder<LinkEvent> esBuilder = new ElasticsearchSink.Builder<LinkEvent>(
                httpHosts,
                new ElasticsearchSinkFunction<LinkEvent>() {

                    IndexRequest createIndexRequest(LinkEvent element){
                        Link link = ((JSONObject)element.getObj()).toJavaObject(Link.class);

                        ESLink esLink = new ESLink();
                        esLink.setName(link.getName());
                        esLink.setTitle(link.getTitle());
                        esLink.setTableId(link.getId().toString());
                        esLink.setUserId(link.getUserId().toString());
                        esLink.setCreatetime(link.getCreateTime());
                        esLink.setUrl(link.getUrl());

                        IndexRequest request = new IndexRequest("loadstar", "links");

                        return request;
                    }

                    @Override
                    public void process(LinkEvent s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(s));
                    }
                }
        );

        return null;
    }
}
