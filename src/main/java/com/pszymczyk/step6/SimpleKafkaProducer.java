package com.pszymczyk.step6;

import io.confluent.examples.clients.basicavro.Payment;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SimpleKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaProducer.class);

    private final KafkaProducer<String, Payment> kafkaProducer;
    private final String topic;

    public SimpleKafkaProducer(String topic) {
        var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProperties.put("schema.registry.url","http://localhost:8081");
        this.kafkaProducer = new KafkaProducer<>(producerProperties);
        this.topic = topic;
    }


    public RecordMetadata syncSend(String messageValue, long timeout, TimeUnit timeUnit) {
        Payment payment = new Payment();
        payment.setId("123");
        payment.setAmount(1000.2);
        var record = new ProducerRecord<String, Payment>(topic, payment);
        try {
            return kafkaProducer.send(record).get(timeout, timeUnit);
        } catch (Exception e) {
            logger.error("Exception while sending message in async mode", e);
            return null;
        }
    }

}
