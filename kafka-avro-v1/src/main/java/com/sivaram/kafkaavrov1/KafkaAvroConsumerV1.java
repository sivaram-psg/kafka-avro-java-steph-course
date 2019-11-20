package com.sivaram.kafkaavrov1;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumerV1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","my-avro-consumer");
        properties.setProperty("enable.auto.commit","false");
        properties.setProperty("auto.offset.reset","earliest");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url","http://127.0.0.1:8081");
        properties.setProperty("specific.avro.reader","true");

        KafkaConsumer<String,Customer> kafkaConsumer = new KafkaConsumer<String, Customer>(properties);
        String topic = "customer-avro";
        kafkaConsumer.subscribe(Collections.singleton(topic));
        System.out.println("starting to get records");
        while(true){
            ConsumerRecords<String,Customer> consumerRecords = kafkaConsumer.poll(500);
            for(ConsumerRecord<String,Customer> consumerRecord :consumerRecords){
                Customer customer = consumerRecord.value();
                System.out.println(customer);
            }
            kafkaConsumer.commitSync();
        }

    }
}
