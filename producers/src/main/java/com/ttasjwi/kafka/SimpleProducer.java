package com.ttasjwi.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class SimpleProducer {

    public static void main(String[] args) {
        // 프로듀서 설정
        KafkaProducer<String, String> kafkaProducer = setupKafkaProducer();

        // 토픽명
        String topicName = "simple-topic";

        // 보낼 메시지(레코드)
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,  "hello world");

        // 메시지 전송
        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private static KafkaProducer<String, String> setupKafkaProducer() {
        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }
}
