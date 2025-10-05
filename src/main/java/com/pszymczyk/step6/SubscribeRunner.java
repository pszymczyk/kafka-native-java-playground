package com.pszymczyk.step6;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG;

class SubscribeRunner {

    private static final Logger logger = LoggerFactory.getLogger(SubscribeRunner.class);

    public static void main(String[] args) {
        var groupId = "step6";
        var topic = "my-queue";

        var consumer0 = sharedConsumerInstance(groupId, topic);
        var consumer1 = sharedConsumerInstance(groupId, topic);
        var consumer2 = sharedConsumerInstance(groupId, topic);

        var consumer0Thread = new Thread(consumer0::start, "consumer-0-thread");
        var consumer1Thread = new Thread(consumer1::start, "consumer-1-thread");
        var consumer2Thread = new Thread(consumer2::start, "consumer-2-thread");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            wakeUpConsumer("0", consumer0, consumer0Thread);
            wakeUpConsumer("1", consumer1, consumer1Thread);
            wakeUpConsumer("2", consumer2, consumer2Thread);
        }, "shutdown-hook-thread"));

        logger.info("Starting consumer thread 0...");
        consumer0Thread.start();
        logger.info("Consumer thread 0 started.");
        logger.info("Starting consumer thread 1...");
        consumer1Thread.start();
        logger.info("Consumer thread 1 started.");
        logger.info("Starting consumer thread 2...");
        consumer2Thread.start();
        logger.info("Consumer thread 2 started.");
    }

    private static class ShareConsumerLoop {

        private final ShareConsumer<String, String> consumer;
        private final String topic;

        public ShareConsumerLoop(String groupId, String topic) {
            this.topic = topic;
            var props = new Properties();
            props.put(BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
            props.put(GROUP_ID_CONFIG, groupId);
            props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
            this.consumer = new KafkaShareConsumer<>(props);
        }

        public void start() {
            consumer.subscribe(List.of(topic));

            try {
                while (true) {
                    var records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Consumer thread: {}, ConsumerRecord: {}",
                                Thread.currentThread().getName(),
                                Map.of(
                                        "partition", record.partition(),
                                        "offset", record.offset(),
                                        "key", Objects.toString(record.key()),
                                        "value", record.value()));
                        consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                    }
                }
            } catch (WakeupException wakeupException) {
                logger.info("Handling WakeupException.");
            } finally {
                logger.info("Closing Kafka consumer...");
                consumer.close();
                logger.info("Kafka consumer closed.");
            }
        }

        public void wakeup() {
            consumer.wakeup();
        }
    }

    private static ShareConsumerLoop sharedConsumerInstance(String groupId, String topic) {
        return new ShareConsumerLoop(groupId, topic);
    }

    private static void wakeUpConsumer(String consumerName, ShareConsumerLoop shareConsumerLoop, Thread thread) {
        logger.info("Hello consumer {}, wakeup!", consumerName);
        shareConsumerLoop.wakeup();
        try {
            thread.join();
        } catch (InterruptedException e) {
            logger.error("Consumer waking up exception.", e);
            throw new RuntimeException(e);
        }
    }
}
