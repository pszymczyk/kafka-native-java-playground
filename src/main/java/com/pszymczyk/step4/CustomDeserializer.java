package com.pszymczyk.step4;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomDeserializer implements Deserializer<Customer> {

    private static final Logger logger = LoggerFactory.getLogger(CustomDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Customer deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Customer.class);
        } catch (Exception e) {
            logger.error("Cannot deserialize given data {} to {}", new String(data), Customer.class, e);
            return null;
        }
    }
}
