package com.pszymczyk;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WslPreWorkshopTest {

    static AdminClient adminClient;

    @BeforeAll
    static void setup() {
         adminClient = AdminClient.create(Map.of(
            "bootstrap.servers", "[::1]:9092",
            "group.id", "create-topics-admin"));
    }

    @AfterAll
    static void cleanup() {
        if (adminClient!= null) {
            adminClient.close();
        }
    }

    @Test
    void shouldCreateTopic() throws Exception{
        adminClient.listTopics().names().get(5, TimeUnit.SECONDS);
    }
}
