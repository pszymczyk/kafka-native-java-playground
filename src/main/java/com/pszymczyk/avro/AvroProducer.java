package com.pszymczyk.avro;

import com.pszymczyk.avro.events.Pageview;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AvroProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, Pageview> producer = new KafkaProducer<>(props);

        Pageview pageview = new Pageview();
        pageview.setIsSpecial(true);
        pageview.setUrl("http://szkoleniekafka.pl");
        pageview.setCustomerId("pszymczyk");
        producer.send(new ProducerRecord<>("pageviews", pageview)).get();

        producer.close();
    }
}
