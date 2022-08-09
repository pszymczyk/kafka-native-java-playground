package com.pszymczyk.step7;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SimpleKafkaProducerWithCustomPartitioner {

    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaProducerWithCustomPartitioner.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    public SimpleKafkaProducerWithCustomPartitioner(String topic) {
        var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VipClientsPartitioner.class);
        this.kafkaProducer = new KafkaProducer<>(producerProperties);
        this.topic = topic;
    }

    public RecordMetadata syncSend(String key, String messageValue, long timeout, TimeUnit timeUnit) {
        var record = new ProducerRecord<>(topic, key, messageValue);
        try {
            return kafkaProducer.send(record).get(timeout, timeUnit);
        } catch (Exception e) {
            logger.error("Exception while sending message in sync mode", e);
            return null;
        }
    }
}
