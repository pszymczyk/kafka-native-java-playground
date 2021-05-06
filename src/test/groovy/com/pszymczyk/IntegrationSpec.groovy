package com.pszymczyk


import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

abstract class IntegrationSpec extends Specification {

    protected static Logger logger = LoggerFactory.getLogger(IntegrationSpec.class)
    protected static String bootstrapServers


    def setupSpec() {
        KafkaContainerStarter.start()
        bootstrapServers = KafkaContainerStarter.bootstrapServers
        JsonPathConfiguration.configure()
    }
}
