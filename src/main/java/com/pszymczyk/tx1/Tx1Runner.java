package com.pszymczyk.tx1;

import com.pszymczyk.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class Tx1Runner {

    private static final Logger logger = LoggerFactory.getLogger(Tx1Runner.class);

    public static void main(String[] args) {
        var topic = "tx1";

        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BusinessTransactionSerializer.class);
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "Tx1Runner_mb-pro");
        final var kafkaProducer = new KafkaProducer<String, BusinessTransaction>(producerProperties);
        kafkaProducer.initTransactions();

        Utils.readLines("warszawa-krucza49_24-10-2023.csv").forEach(line -> {
            try {
                kafkaProducer.beginTransaction();

                String[] split = line.split(",");
                String buyer = split[1];
                String seller = split[2];
                String stock = split[3];
                int number = Integer.valueOf(split[4]);

                ProducerRecord<String, BusinessTransaction> buy = new ProducerRecord<>(
                    topic, buyer, BusinessTransaction.buy(buyer, stock, number));
                kafkaProducer.send(buy, (metadata, exception) -> {
                    if (metadata != null) {
                        logger.info("Record sent, {}.", metadata);
                    } else {
                        logger.error("Record sending failed. ", exception);
                    }
                }).get();

                Utils.failSometimes();

                ProducerRecord<String, BusinessTransaction> sell = new ProducerRecord<>(
                    topic, buyer, BusinessTransaction.sell(seller, stock, number));
                kafkaProducer.send(sell, (metadata, exception) -> {
                    if (metadata != null) {
                        logger.info("Record sent, {}.", metadata);
                    } else {
                        logger.error("Record sending failed. ", exception);
                    }
                }).get();

                kafkaProducer.commitTransaction();
            } catch (Exception e) {
                kafkaProducer.abortTransaction();
            }
        });
    }
}
