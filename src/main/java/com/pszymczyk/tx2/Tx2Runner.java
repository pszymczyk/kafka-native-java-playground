package com.pszymczyk.tx2;

import com.pszymczyk.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

class Tx2Runner {

    private static final Logger logger = LoggerFactory.getLogger(Tx2Runner.class);

    public static void main(String[] args) {
        var inputTopic = "tx2-input";
        var outputTopic = "tx2-output";
        var groupId = "tx2-input";

        final var props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        final var kafkaConsumer = new KafkaConsumer<String, String>(props);

        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BusinessTransactionSerializer.class);
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "Tx2Runner_mb-pro");
        final var kafkaProducer = new KafkaProducer<String, BusinessTransaction>(producerProperties);
        kafkaProducer.initTransactions();
        kafkaConsumer.subscribe(List.of(inputTopic));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (var record : records) {

                try {

                    kafkaProducer.beginTransaction();
                    String[] split = record.value().split(",");
                    String buyer = split[1];
                    String seller = split[2];
                    String stock = split[3];
                    int number = Integer.valueOf(split[4]);

                    kafkaProducer.sendOffsetsToTransaction(Map.of(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset())), kafkaConsumer.groupMetadata());

                    ProducerRecord<String, BusinessTransaction> buy = new ProducerRecord<>(outputTopic, buyer, BusinessTransaction.buy(buyer, stock
                        , number));
                    kafkaProducer.send(buy, (metadata, exception) -> {
                        if (metadata != null) {
                            logger.info("Record sent, {}.", metadata);
                        } else {
                            logger.error("Record sending failed. ", exception);
                        }
                    });

                    Utils.failSometimes();

                    ProducerRecord<String, BusinessTransaction> sell = new ProducerRecord<>(outputTopic, buyer, BusinessTransaction.sell(seller,
                        stock, number));
                    kafkaProducer.send(sell, (metadata, exception) -> {
                        if (metadata != null) {
                            logger.info("Record sent, {}.", metadata);
                        } else {
                            logger.error("Record sending failed. ", exception);
                        }
                    });

                    kafkaProducer.commitTransaction();
                } catch (Exception exception) {
                    kafkaProducer.abortTransaction();
                    kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                    logger.error("Exception during message processing.", exception);
                }
            }
        }
    }
}
