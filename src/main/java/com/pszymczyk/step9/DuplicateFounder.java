package com.pszymczyk.step9;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class DuplicateFounder {

    public static void main(String[] args) {
        String topic = "step9";
        ExecutorService executor = Executors.newSingleThreadExecutor();
        DuplicateFounderConsumer duplicateFounderConsumer = new DuplicateFounderConsumer(topic);
        executor.submit(duplicateFounderConsumer);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            duplicateFounderConsumer.shutdown();
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    static public class DuplicateFounderConsumer implements Runnable {

        private final KafkaConsumer<String, String> consumer;
        private final String topic;

        private final Set<Integer> map = new HashSet<>();

        public DuplicateFounderConsumer(String topic) {
            this.topic = topic;
            Properties props = new Properties();
            props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(GROUP_ID_CONFIG, "step5_"+ UUID.randomUUID().toString().substring(0, 7));
            props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<>(props);
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(Arrays.asList(topic));

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                    for (ConsumerRecord<String, String> record : records) {
                        String[] split = record.value().split("\\.");
                        Integer i = Integer.valueOf(split[0]);
                        if (map.contains(i)) {
                            System.err.println("Duplicate on item " + record.value());
                        }
                        map.add(i);
                    }
                }
            } catch (WakeupException e) {
                // ignore for shutdown
            } finally {
                consumer.close();
            }
        }


        public void shutdown() {
            consumer.wakeup();
        }


    }
}
