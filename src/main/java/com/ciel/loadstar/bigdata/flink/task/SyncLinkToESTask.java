package com.ciel.loadstar.bigdata.flink.task;

import com.alibaba.fastjson.JSONObject;
import com.ciel.loadstar.bigdata.flink.config.ElkConfig;
import com.ciel.loadstar.bigdata.flink.config.ElkHost;
import com.ciel.loadstar.bigdata.flink.config.KafkaUtil;
import com.ciel.loadstar.bigdata.flink.domain.ESLink;
import com.ciel.loadstar.bigdata.flink.domain.Link;
import com.ciel.loadstar.bigdata.flink.domain.LinkEvent;
import com.ciel.loadstar.bigdata.flink.map.LinkEventMapFunction;
import com.ciel.loadstar.bigdata.flink.util.ESRestClient;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
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
    static String kafkaTopic = "LinkEvent_Dev";
    static String esHost = "loadstar-8738674613.ap-southeast-2.bonsaisearch.net";
    static Integer esPort = 443;
    static String esSchema = "https";
    static String esUsername = "bfsr68vnuo";
    static String esPassword = "d3k85wu86d";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties kafkaProperties = KafkaUtil.getProperties();
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>(kafkaTopic, new SimpleStringSchema(), kafkaProperties);
        consumer.setStartFromLatest();

        DataStreamSource<String> dataStreamSource = env.addSource(consumer);
        DataStream dataStream = dataStreamSource.map(new LinkEventMapFunction());

        ArrayList<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost(esHost, esPort, esSchema));

        ElkHost elkHost = new ElkHost();
        elkHost.setIp(esHost);
        elkHost.setSchema(esSchema);
        elkHost.setPort(esPort.toString());
        elkHost.setUsername(esUsername);
        elkHost.setPassword(esPassword);

        List<ElkHost> elkHosts = new ArrayList<>();
        elkHosts.add(elkHost);

        ElkConfig elkConfig = new ElkConfig();
        elkConfig.setHosts(elkHosts);
        elkConfig.setClustername("loadstar");

        ESRestClient.elkConfig = elkConfig;

        ElasticsearchSink.Builder<LinkEvent> elasticsearchSinkBuilder = new ElasticsearchSink.Builder<LinkEvent>(
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
        );

        HttpHost[] array =new HttpHost[1];
        RestClientFactoryImpl restClientFactory = new RestClientFactoryImpl("bfsr68vnuo", "d3k85wu86d");
        restClientFactory.configureRestClientBuilder(RestClient.builder(httpHosts.toArray(array)));
        elasticsearchSinkBuilder.setRestClientFactory(restClientFactory);
        elasticsearchSinkBuilder.setBulkFlushMaxActions(1);

        dataStream.addSink(elasticsearchSinkBuilder.build());
        dataStream.addSink(new PrintSinkFunction());

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
