package com.pszymczyk.step10;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class LoanApplicationDeserializer implements Deserializer<LoanApplicationRequest> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public LoanApplicationRequest deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, LoanApplicationRequest.class);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}
