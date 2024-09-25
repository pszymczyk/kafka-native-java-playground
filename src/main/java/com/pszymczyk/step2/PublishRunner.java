package com.pszymczyk.step2;

import com.pszymczyk.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.Random;

class PublishRunner {

    public static final String TOPIC = "step2";
    private static final Logger logger = LoggerFactory.getLogger(PublishRunner.class);
    private static final Random random = new Random();

    public static void main(String[] args) {

        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final var kafkaProducer = new KafkaProducer<String, String>(producerProperties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaProducer::close, "shutdown-hook-thread"));

        random.ints(0, 100_000).mapToObj(Objects::toString).forEach(i -> {
            Utils.sleeep(100);
            //TODO 1/1 create producer record where key:${randomNumber}|value:My favourite number is ${randomNumber}
            ProducerRecord<String, String> record = null;
                kafkaProducer.send(record, (metadata, exception) -> {
                    if (metadata != null) {
                        logger.info("Message sent metadata: {}", metadata);
                    } else {
                        logger.error("Error ", exception);
                    }
                });
            }
        );
    }
}
