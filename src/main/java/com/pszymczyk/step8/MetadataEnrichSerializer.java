package com.pszymczyk.step8;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

public class MetadataEnrichSerializer implements Serializer<String> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, String data) {
        return new byte[0];
    }

    @Override
    public byte[] serialize(String topic, Headers headers, String data) {
        headers.add(new RecordHeader("local_time", LocalDateTime.now(ZoneId.of("Canada/Yukon")).toString().getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("source", "Step8_microservice".getBytes(StandardCharsets.UTF_8)));
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
