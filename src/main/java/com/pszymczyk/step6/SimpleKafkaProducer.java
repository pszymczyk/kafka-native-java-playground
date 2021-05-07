package com.pszymczyk.step6;

import com.pszymczyk.ConsumerLoop;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SimpleKafkaProducer {

    protected static Logger logger = LoggerFactory.getLogger(ConsumerLoop.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    public SimpleKafkaProducer(String topic) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.kafkaProducer = new KafkaProducer<>(producerProperties);
        this.topic = topic;
    }

    public void sendAndForget(String messageValue) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageValue);
        try {
            kafkaProducer.send(record);
        } catch (Exception e) {
            logger.warn("Exception while sending message in send and forget mode", e);
        }
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

    public Future<RecordMetadata> asyncSend(String messageValue, Callback callback) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageValue);
        try {
            return kafkaProducer.send(record, callback);
        } catch (Exception e) {
            logger.error("Exception while sending message in sync mode", e);
            return CompletableFuture.completedFuture(null);
        }
    }
}
