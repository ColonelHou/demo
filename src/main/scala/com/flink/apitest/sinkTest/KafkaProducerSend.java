package com.flink.apitest.sinkTest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerSend {
    public static void main(String[] args) {
        Properties props = new Properties();
        String topic = "test";
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        ProducerRecord record = null;
        for (int i = 10010; i < 10030; i ++) {
            record = new ProducerRecord<String, String>(topic, "abc-" + i +",def-" + i);
            producer.send(record);
        }
        producer.close();
    }
}
