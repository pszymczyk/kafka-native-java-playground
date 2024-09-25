package com.pszymczyk.avro;


import com.pszymczyk.avro.events.Pageview;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class AvroConsumer {

    private static final Logger logger = LoggerFactory.getLogger(AvroConsumer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_VALUE_TYPE_CONFIG, Pageview.class.getName());

        String topic = "pageviews";
        final Consumer<String, Pageview> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));

        try {
            while (true) {
                ConsumerRecords<String, Pageview> records = consumer.poll(1000);
                for (ConsumerRecord<String, Pageview> record : records) {
                    Pageview pageview = record.value();
                    logger.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), pageview);
                }
                if (!records.isEmpty()) {
                    break;
                }
            }
        } finally {
            consumer.close();
        }
    }
}
