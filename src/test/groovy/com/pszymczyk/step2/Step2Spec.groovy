package com.pszymczyk.step2

import com.pszymczyk.IntegrationSpec
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import spock.util.concurrent.PollingConditions

import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class Step2Spec extends IntegrationSpec {

    def "Should publish and consume some messages"() {
        given:

            String topic = "publish-test-topic"

            ExecutorService consumerThread = Executors.newSingleThreadExecutor()
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "consumer-tutorial");
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());

            consumerThread.submit({
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                    consumer.subscribe(Arrays.asList(topic))
                    while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100))
                        for (ConsumerRecord<String, String> record : records) {
                            logger.info("topic = %s, partition = %s, offset = %d, key %, payload %s",
                                    record.topic(),
                                    record.partition(),
                                    record.key(),
                                    record.value())
                        }
                    }
                }
            })

            Step2Runner runner = new Step2Runner(bootstrapServers)
        when:
            runner.publishMessages(topic)
        then:
            new PollingConditions(timeout: 30).eventually {
                runner.readMessages(topic)
            }
        cleanup:
            consumerThread.shutdown()
    }
}
