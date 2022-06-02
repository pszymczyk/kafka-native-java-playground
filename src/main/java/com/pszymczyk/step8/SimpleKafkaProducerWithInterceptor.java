package com.pszymczyk.step8;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SimpleKafkaProducerWithInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaProducerWithInterceptor.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    public SimpleKafkaProducerWithInterceptor(String topic) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MetadataEnrichmentInterceptor.class.getName());
        this.kafkaProducer = new KafkaProducer<>(producerProperties);
        this.topic = topic;
    }

    public RecordMetadata syncSend(String messageValue, long timeout, TimeUnit timeUnit) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageValue);
        try {
            return kafkaProducer.send(record).get(timeout, timeUnit);
        } catch (Exception e) {
            logger.error("Exception while sending message in sync mode", e);
            return null;
        }
    }
}
