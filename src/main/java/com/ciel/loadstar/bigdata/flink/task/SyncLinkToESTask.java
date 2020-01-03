package com.ciel.loadstar.bigdata.flink.task;

import com.alibaba.fastjson.JSONObject;
import com.ciel.loadstar.bigdata.flink.config.*;
import com.ciel.loadstar.bigdata.flink.domain.ESLink;
import com.ciel.loadstar.bigdata.flink.domain.Link;
import com.ciel.loadstar.bigdata.flink.map.LinkEventMapFunction;
import com.ciel.loadstar.bigdata.flink.util.ESRestClient;
import com.ciel.loadstar.infrastructure.events.link.LinkEvent;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SyncLinkToESTask {
    public static void main(String[] args) throws Exception {
        String linkEventTopic = NacosUtil.getProperty(ConfigConstant.NACOS_TOPIC_LINK_EVENT);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties kafkaProperties = KafkaUtil.getProperties();
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>(linkEventTopic, new SimpleStringSchema(), kafkaProperties);
        consumer.setStartFromGroupOffsets();

        DataStreamSource<String> dataStreamSource = env.addSource(consumer);
        DataStream dataStream = dataStreamSource.map(new LinkEventMapFunction());

        ElkConfig elkConfig = EsConfigUtil.getConfig();
        ArrayList<HttpHost> httpHosts = EsConfigUtil.getHttpHosts();
        ESRestClient.elkConfig = elkConfig;

        ElasticsearchSink.Builder<LinkEvent> elasticsearchSinkBuilder = new ElasticsearchSink.Builder<LinkEvent>(
                httpHosts,
                new ElasticsearchSinkFunction<LinkEvent>() {

                    IndexRequest createIndexRequest(LinkEvent element){
                        Link link = ((JSONObject)element.getObj()).toJavaObject(Link.class);

                        ESLink esLink = new ESLink();
                        esLink.setProfile(element.getProfile());
                        esLink.setName(link.getName());
                        esLink.setTitle(link.getTitle());
                        esLink.setTableId(link.getId().toString());
                        esLink.setUserId(link.getUserId().toString());
                        esLink.setCreatetime(link.getCreateTime());
                        esLink.setUrl(link.getUrl());
                        IndexRequest request = new IndexRequest("links", "_doc");
                        request.source(JSONObject.toJSONString(esLink), XContentType.JSON);
                        System.out.println("index doc id " + link.getId());

                        return request;
                    }

                    List<DeleteRequest> createDeleteRequest(LinkEvent linkEvent){
                        List<DeleteRequest> deleteRequests = new ArrayList<>();

                        Link link = ((JSONObject)linkEvent.getObj()).toJavaObject(Link.class);

                        RestHighLevelClient client = ESRestClient.getClient();

                        SearchRequest searchRequest = new SearchRequest("links");

                        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
                        TermQueryBuilder tableIdTermQuery = QueryBuilders.termQuery("tableId", link.getId());
                        boolQueryBuilder.must(tableIdTermQuery);

                        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                        searchSourceBuilder.query(boolQueryBuilder);
                        searchRequest.source(searchSourceBuilder);

                        try {
                            SearchResponse searchResponse = client.search(searchRequest);
                            if (searchResponse.status() == RestStatus.OK){
                                SearchHits hits = searchResponse.getHits();
                                SearchHit[] searchHits = hits.getHits();
                                for (SearchHit hit : searchHits) {
                                    String id = hit.getId();
                                    deleteRequests.add(new DeleteRequest("links", "_doc", id));
                                }
                            }
                            System.out.println("delete doc id " + link.getId());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        return deleteRequests;
                    }

                    @Override
                    public void process(LinkEvent linkEvent, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        if (linkEvent != null){
                            switch (linkEvent.getEventType()){
                                case "NEW":
                                    requestIndexer.add(createIndexRequest(linkEvent));
                                    break;
                                case "DELETE":
                                    createDeleteRequest(linkEvent).forEach(req -> requestIndexer.add(req));
                                    break;
                            }
                        }
                    }
                }
        );

        HttpHost[] array =new HttpHost[1];
        RestClientFactoryImpl restClientFactory = new RestClientFactoryImpl(NacosUtil.getProperty(ConfigConstant.NACOS_ES_USERNAME), NacosUtil.getProperty(ConfigConstant.NACOS_ES_PASSWORD));
        restClientFactory.configureRestClientBuilder(RestClient.builder(httpHosts.toArray(array)));
        elasticsearchSinkBuilder.setRestClientFactory(restClientFactory);
        elasticsearchSinkBuilder.setBulkFlushMaxActions(1);

        dataStream.addSink(elasticsearchSinkBuilder.build());
//        dataStream.addSink(new PrintSinkFunction());

        env.execute("SyncLinkToESTask");
    }

    /**
     * 配置${@link org.elasticsearch.client.RestHighLevelClient}
     * 添加权限认证、设置超时时间等等
     * 实现${@link RestClientFactory#configureRestClientBuilder(RestClientBuilder)}方法
     */
    static class RestClientFactoryImpl implements RestClientFactory {

        private String username;

        private String password;

        private RestClientFactoryImpl(String username, String password) {
            this.username = username;
            this.password = password;
        }

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
            basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));

            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider));
        }
    }
}
