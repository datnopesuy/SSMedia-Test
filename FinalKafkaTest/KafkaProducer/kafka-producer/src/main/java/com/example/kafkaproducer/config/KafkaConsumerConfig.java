//package com.example.kafkaproducer.config;
//
//import com.example.kafkaproducer.dto.TransactionEvent;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.SerializationFeature;
//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
//import org.springframework.kafka.core.ConsumerFactory;
//import org.springframework.kafka.listener.ContainerProperties;
//
//@Configuration
//public class KafkaConsumerConfig {
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, TransactionEvent>
//            kafkaListenerContainerFactory(ConsumerFactory<String, TransactionEvent> consumerFactory) {
//        ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> factory =
//                new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory);
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
//        return factory;
//    }
//
//    @Bean
//    public ObjectMapper objectMapper() {
//        ObjectMapper mapper = new ObjectMapper();
//        mapper.registerModule(new JavaTimeModule());
//        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//        return mapper;
//    }
//}
