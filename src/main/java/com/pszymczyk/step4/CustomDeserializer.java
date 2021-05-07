package com.pszymczyk.step4;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomDeserializer implements Deserializer<Customer> {

    private static Logger logger = LoggerFactory.getLogger(CustomDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Customer deserialize(String topic, byte[] data) {
        try {
            Customer customer = objectMapper.readValue(data, Customer.class);
            return customer;
        } catch (Exception e) {
            logger.error("cannot deserialize customer", e);
            return null;
        }
    }

}
