package com.pszymczyk.tx1;

import com.pszymczyk.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Tx1Runner {
    private static final Logger logger = LoggerFactory.getLogger(Tx1Runner.class);

    public static void main(String[] args) {
        var topic = "tx1";

        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BusinessTransactionSerializer.class);
        //TODO 1/2 Configure Kafka Producer transactions
        final var kafkaProducer = new KafkaProducer<String, BusinessTransaction>(producerProperties);

        Utils.readLines("warszawa-krucza49_24-10-2023.csv").forEach(line -> {
            // TODO 2/2 Send two separate messages in one transaction: SELL and BUY
        });
    }
}
