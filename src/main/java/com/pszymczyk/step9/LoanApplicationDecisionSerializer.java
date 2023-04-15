package com.pszymczyk.step9;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LoanApplicationDecisionSerializer implements Serializer<LoanApplicationDecision> {

    private static final Logger logger = LoggerFactory.getLogger(LoanApplicationDecisionSerializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, LoanApplicationDecision data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (IOException e) {
            logger.error("Error while serializing {} to byte[]", LoanApplicationDecision.class);
            throw new SerializationException(e);
        }
    }
}
