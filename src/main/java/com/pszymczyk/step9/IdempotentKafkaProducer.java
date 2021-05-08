package com.pszymczyk.step9;

import com.pszymczyk.ConsumerLoop;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class IdempotentKafkaProducer {

    protected static Logger logger = LoggerFactory.getLogger(ConsumerLoop.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    public IdempotentKafkaProducer(String topic) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //disable to detect some duplicates
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        this.kafkaProducer = new KafkaProducer<>(producerProperties);
        this.topic = topic;
    }

    public void asyncSend(String messageValue) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageValue);
        Callback callback = (metadata, exception) -> logger.info("Message sent completed, record metadata: {}, exception: ", metadata, exception);
        try {
            kafkaProducer.send(record, callback);
        } catch (Exception e) {
            logger.error("Exception while sending message in sync mode", e);
        }
    }
}
