package kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.format.ResolverStyle;
import java.util.Arrays;
import java.util.Properties;

public class ElasticsearchConsumer {

    // create Elasticsearch client
    public static RestHighLevelClient createClient() {

        String hostname = "search-kafka-tweets-rfe4jokql6iulqope5kfwtzqvu.us-east-1.es.amazonaws.com";
//        String username = "";
//        String password = "";

        // only for not-local deployments: create credentials provider
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//         credentialsProvider.setCredentials(AuthScope.ANY),
//        new UsernamePasswordCredentials(username, password));

        // create REST client builder (configure secure connections over https and credentials
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"));
//                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                     @Override
//                     public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                         return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                     }
//                 });

        // create REST client
        RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }

    // create kafka consumer
    public static KafkaConsumer<String, String> createConsumer(String topic) {

        String bootstrapServer = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }


    public static String extractIdFromTweet(String tweetJson) {

        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {

        // create logger
        Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());

        // create elasticsearch client
        RestHighLevelClient client = createClient();

        // Kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        // poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            // create batches
            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {

                // idempotency strategy 1: kafka generic id: in case when you can't find a unique id
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                try {
                    // idempotency strategy 2: twitter feed specific id
                    String id = extractIdFromTweet(record.value());

                    // index request
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id // for idempotent consumer
                    ).source(record.value(), XContentType.JSON);

                    // add to the bulk request
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e){
                    logger.info("Skipping bad data: " + record.value());
                }

//                // index response (only if we are not batching)
//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//                logger.info(indexResponse.getId());
            }

            if (recordCount != 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // close the client
//        client.close();
    }
}
