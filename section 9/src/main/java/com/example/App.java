package com.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Hello world!
 *
 */
public class App 
{
    private static final Logger log = LoggerFactory.getLogger(App.class.getSimpleName());
    static String topic = "LoremTopic";
    static String value = "ipsumValue";
    public static void main( String[] args )
    {
        log.info("BEGIN");

        //Create Producer Properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //Create a producer record
        ProducerRecord<String,String> producerRecord = 
            new ProducerRecord<String,String>(topic,value);

        //send the data (async)
        producer.send(producerRecord);
        //flush data (sync) 
        producer.flush();
        //close the producer
        producer.close();
        log.info("END");
    }
}

