package com.datastream;
import com.datastream.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello world!");
        
        String brokers = "localhost:9092";
        String groupId = "test-group";
        String topic = "test-topic";

        Consumer consumerRunnable = new Consumer(brokers, groupId, topic);
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();
    }
}