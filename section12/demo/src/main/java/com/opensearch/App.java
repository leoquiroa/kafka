package com.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


/**
 * Hello world!
 *
 */
public class App 
{
    static String connNoSqlDb = "http://172.19.0.4:9200";
    static String indexName = "wikimedia";

    static String connKafkaTopics = "localhost:9092";
    static String topicName = "wikimedia.recentchange";
    static String groupId = "grupo-firme";

    
    public static RestHighLevelClient createOpenSearchClient() {
        //String connString = "http://localhost:9200";
        //String connString = "http://172.19.0.2:9200";
        // String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connNoSqlDb);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(
                    new HttpHost(
                        connUri.getHost(), 
                        connUri.getPort(), 
                        "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                        new HttpHost(
                            connUri.getHost(), 
                            connUri.getPort(), 
                            connUri.getScheme()))
                        .setHttpClientConfigCallback(
                                h -> h.setDefaultCredentialsProvider(cp)
                                        .setKeepAliveStrategy(
                                            new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connKafkaTopics);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);

    }

    public static void main( String[] args ) throws IOException{
        // create the log object
        Logger log = LoggerFactory.getLogger(App.class.getSimpleName());
        // first create an OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        // create our Kafka Client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        

        // we need to create the index on OpenSearch if it doesn't exist already
        try{
            boolean theIndexExist = openSearchClient.indices().exists(
                new GetIndexRequest(indexName), 
                RequestOptions.DEFAULT);
            if (theIndexExist){
                log.info("The topic " + indexName + " already exist");                
            } else {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The topic " + indexName + " was created");
            }
        }catch(Exception e){
            log.error(e.toString());
            log.info("Error creating the topic " + indexName);
        }

        try {
            // we subscribe the consumer
            consumer.subscribe(Collections.singleton(topicName));
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                if (recordCount == 0) break;
                log.info("Received " + recordCount + " record(s)");
            }
        } catch (Exception e) {
            log.error(e.toString());
            log.info("Error getting data from the topic " + indexName);
        }
    }
}

//88    .7
//95    .75
//101   .8
