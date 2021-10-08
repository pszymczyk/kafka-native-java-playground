package com.pszymczyk.step4;

public class Step4Runner {

    public static void main(String[] args) {
        String groupId = "step4";
        String topic = "step4";
        try (var consumer = new ConsumerLoopWithCustomDeserializer(groupId, topic)){
            consumer.start();
        }
    }
}
