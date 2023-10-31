package com.pszymczyk.step3;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

class MetadataEnrichmentInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        producerRecord.headers().add(new RecordHeader("local_time",
            LocalDateTime.now(ZoneId.of("Canada/Yukon")).toString().getBytes(StandardCharsets.UTF_8)));
        producerRecord.headers().add(new RecordHeader("source", "Step3_microservice".getBytes(StandardCharsets.UTF_8)));
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
