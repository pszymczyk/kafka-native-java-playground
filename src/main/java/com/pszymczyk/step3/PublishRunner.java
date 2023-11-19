package com.pszymczyk.step3;

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

    public static final String TOPIC = "step3";
    private static final Logger logger = LoggerFactory.getLogger(PublishRunner.class);

    public static void main(String[] args) {

        final var random = new Random();
        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, PositiveNegativePartitioner.class);
        producerProperties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MetadataEnrichmentInterceptor.class.getName());
        final var kafkaProducer = new KafkaProducer<String, String>(producerProperties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaProducer::close, "shutdown-hook-thread"));

        random.ints(-100, 100).mapToObj(Objects::toString).forEach(i -> {
            Utils.sleeep(100);
            final var record = new ProducerRecord<String, String>(TOPIC, i);
            kafkaProducer.send(record, (metadata, exception) -> {
                if (metadata != null) {
                    logger.info("Message sent metadata: {}", metadata);
                } else {
                    logger.error("Error ", exception);
                }
            });
        });
    }

}
