package com.pszymczyk.step10;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class LoanApplicationDecisionSerializer implements Serializer<LoanApplicationDecision> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, LoanApplicationDecision data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}
