package com.pszymczyk.step1;

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
import java.util.stream.IntStream;

public class PublishRunner {

    public static final String TOPIC = "step1";
    private static final Logger logger = LoggerFactory.getLogger(PublishRunner.class);

    public static void main(String[] args) {

        final var random = new Random();

        //Todo 1/2 - create Kafka Producer
        final KafkaProducer<String, String> kafkaProducer = null;

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaProducer::close, "shutdown-hook-thread"));

        IntStream.generate(() -> random.nextInt(100_000)).mapToObj(Objects::toString).forEach(i -> {
            Utils.sleeep(100);
            //Todo 2/2 - send message "My favourite number is " + i
            }
        );
    }
}
