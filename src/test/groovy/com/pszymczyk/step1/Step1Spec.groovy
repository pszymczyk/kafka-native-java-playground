package com.pszymczyk.step1

import com.pszymczyk.IntegrationSpec
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig

class Step1Spec extends IntegrationSpec {


    def "Should create kafka topics programmatically"() {
        given:
            AdminClient adminClient = AdminClient.create(defaultProperties())
        when:
            new Step1Runner(bootstrapServers).run()
        then:
            adminClient.listTopics()
                    .names()
                    .get()
                    .containsAll("test-topic1", "topic-with-3-partitions")
    }

    Map<String, Object> defaultProperties() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
        return conf
    }
}
