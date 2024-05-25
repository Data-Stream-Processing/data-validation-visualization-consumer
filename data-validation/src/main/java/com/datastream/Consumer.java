package com.datastream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private static final Logger logger = Logger.getLogger(Consumer.class);

    public static KafkaConsumer<String, String> createConsumer(){
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

        return consumer;

    }

    static void runConsumer() throws InterruptedException, JsonParseException{
        final KafkaConsumer<String, String> consumer = createConsumer();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info(String.format("Consumed message: offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()));
                    String prettyJsonRecorValue = DataValidator.transformStringTopPrettyJsonString(record.value());
                    if(prettyJsonRecorValue != null)
                        logger.info("Parsed and validated json: \n"+  prettyJsonRecorValue);
                    else
                        logger.error("Failed to parse JSON: " + record.value());
                }
            }
        } finally {
            consumer.close();
        }

    }

    
    
}
