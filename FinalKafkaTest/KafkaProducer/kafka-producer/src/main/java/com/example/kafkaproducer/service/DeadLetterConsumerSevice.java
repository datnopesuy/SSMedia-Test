//package com.example.kafkaproducer.service;
//
//import com.example.kafkaproducer.dto.TransactionEvent;
//import com.example.kafkaproducer.entity.FailedMessage;
//import com.example.kafkaproducer.repository.FailedMessageRepository;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import lombok.AccessLevel;
//import lombok.RequiredArgsConstructor;
//import lombok.experimental.FieldDefaults;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.support.Acknowledgment;
//import org.springframework.stereotype.Service;
//
//import java.time.LocalDateTime;
//import java.util.UUID;
//
//@Service
//@Slf4j
//@RequiredArgsConstructor
//@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
//public class DeadLetterConsumerSevice {
//
//    FailedMessageRepository failedMessageRepository;
//    ObjectMapper objectMapper;
//
//    @KafkaListener(topics = "transaction-logs-dlq", groupId = "producer-dlq-consumer-group", containerFactory = "kafkaListenerContainerFactory")
//    public void consumerDeadLetterMessages(ConsumerRecord<String, TransactionEvent> record
//            , Acknowledgment acknowledgement){
//        try {
//            log.error("Received message from dead letter queue: key={}, value={}", record.key(), record.value());
//            TransactionEvent event = record.value();
//            FailedMessage deadLetter = FailedMessage.builder()
//                    .id(UUID.randomUUID().toString())
//                    .topicName("transaction-logs-dlq")
//                    .keyValue(event.id())
//                    .payloadJson(objectMapper.writeValueAsString(event))
//                    .errorMessage("Moved to dead letter queue after exeeding retry limit")
//                    .retryCount(999)
//                    .createdAt(LocalDateTime.now())
//                    .build();
//            failedMessageRepository.save(deadLetter);
//            log.warn("Saved dead-letter message {} to DB for manual review", event.id());
//            acknowledgement.acknowledge();
//        }catch (Exception e) {
//            log.error("Error processing dead letter message: " + e.getMessage());
//        }
//    }
//}
