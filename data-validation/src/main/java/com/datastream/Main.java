package com.datastream;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello world!");
        
        String brokers = "localhost:9092";
        String groupId = "test-group";
        String topic = "test-topic";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("group.id", groupId);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(record.value());
                }
            }
        } finally {
            consumer.close();
        }
      
    }
}