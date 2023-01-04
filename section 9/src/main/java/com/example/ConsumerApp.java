package com.example;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Hello world!
 *
 */
public class ConsumerApp 
{
    private static final Logger log = LoggerFactory.getLogger(App.class.getSimpleName());
    static String topic = "topico2125";
    static String value = "lorem ipsum ";
    
    public static void main( String[] args )
    {
        log.info("BEGIN");
        String topic = "LoremTopic";
        String groupId = "sitGroup";
        
        //Create Consumer Properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create the Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        //Subscribe consumer to our topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while(true){
            log.info("Polling");

            ConsumerRecords<String, String> records = 
                consumer.poll(  // poll new data right now
                    Duration.ofMillis(1000)); // but, I am willing to wait for 100 ms
            //if no data is retrieved in the 2-steps verification,
            //then records will be empty
            
            for (ConsumerRecord<String,String> record : records ){
                log.info("Key: " + record.key());
                log.info("Value: " + record.value());
                log.info("Partition: " + record.partition());
                log.info("Offset: " + record.offset());
            }
            
        }
    }
}

