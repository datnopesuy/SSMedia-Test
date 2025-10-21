package com.example.kafkaproducer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic transactionLogsTopic() {
        return TopicBuilder.name("transaction-logs")
                .partitions(3)
                .replicas(3)
                .build();
    }

    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name("transaction-logs-dlq")
                .partitions(3)
                .replicas(3)
                .build();
    }
}
