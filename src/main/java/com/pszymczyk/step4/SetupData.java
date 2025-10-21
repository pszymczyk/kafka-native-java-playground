package com.pszymczyk.step4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

class SetupData {

    private static final Logger logger = LoggerFactory.getLogger(SetupData.class);
    private static final String TOPIC = "step4";
    private static final List<String> littleMissMuffet = List.of(
        "Little Miss Muffet",
        "Sat on a tuffet,",
        "Eating her curds and whey;",
        "There came a big spider,",
        "Who sat down beside her",
        "And frightened Miss Muffet away."
    );

    public static void main(String[] args) throws Exception {

        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final var kafkaProducer = new KafkaProducer<String, String>(producerProperties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaProducer::close, "shutdown-hook-thread"));

        for (var line : littleMissMuffet) {
            kafkaProducer.send(new ProducerRecord<>(TOPIC, 0, null, line), (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error ", exception);
                } else {
                    logger.info("Message sent metadata: {}", metadata);
                }
            }).get();
        }
    }
}
