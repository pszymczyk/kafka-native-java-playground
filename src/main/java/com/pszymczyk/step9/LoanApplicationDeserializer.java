package com.pszymczyk.step9;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LoanApplicationDeserializer implements Deserializer<LoanApplicationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(LoanApplicationDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public LoanApplicationRequest deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, LoanApplicationRequest.class);
        } catch (IOException e) {
            logger.error("Error while deserializing {} to {}}", new String(data), LoanApplicationDecision.class);
            throw new SerializationException(e);
        }
    }
}
