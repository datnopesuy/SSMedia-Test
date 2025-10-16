package com.example.kafkaproducer.service;

import com.example.kafkaproducer.dto.TransactionEvent;
import com.example.kafkaproducer.entity.FailedMessage;
import com.example.kafkaproducer.repository.FailedMessageRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j

public class ProducerService {

    KafkaTemplate<String, TransactionEvent> kafkaTemplate;
    FailedMessageRepository failedMessageRepository;
    ObjectMapper objectMapper;

    private static final String TOPIC = "transaction-logs";
    Random random = new Random();

    @Scheduled(fixedRate = 60_000)
    public void sendTransactionEvent() {
        for (int i = 0; i < 10; i++) {
            try {
                TransactionEvent event = new TransactionEvent(
                        UUID.randomUUID().toString(),
                        LocalDateTime.now(),
                        "user-" + random.nextInt(1000),
                        BigDecimal.valueOf(random.nextDouble() * 1000)
                );
                var record = createProducerRecord(event);

                try {
                    kafkaTemplate.send(record).whenComplete((result, ex) -> {
                        if(ex == null) {
                            log.info("Sent to topic={}, key={}, partition={}",
                                    TOPIC, record.key(), result.getRecordMetadata().partition());
                        } else {
                            log.error("Failed to send message with key={} : {}", record.key(), ex.getMessage());
                            saveFailedMessage(event, ex);
                        }
                    });
                } catch (Exception ex) {
                    // Bắt exception đồng bộ (metadata timeout)
                    log.error("Failed to send message with key={} : {}", record.key(), ex.getMessage());
                    saveFailedMessage(event, ex);
                }
            } catch (Exception ex) {
                // Bắt mọi exception khác để vòng lặp tiếp tục
                log.error("Unexpected error in iteration {}: {}", i, ex.getMessage());
            }
        }
    }

    public ProducerRecord<String, TransactionEvent> createProducerRecord(TransactionEvent transactionEvent) {
        return new ProducerRecord<>(TOPIC, transactionEvent.id(), transactionEvent);
    }

    private void saveFailedMessage(TransactionEvent event, Throwable ex) {
        try {
            String jsonValue = objectMapper.writeValueAsString(event);
            FailedMessage failed = FailedMessage.builder()
                    .id(UUID.randomUUID().toString())
                    .topicName(TOPIC)
                    .keyValue(event.id())
                    .payloadJson(jsonValue)
                    .errorMessage(ex.getMessage())
                    .retryCount(0)
                    .createdAt(LocalDateTime.now())
                    .build();
            failedMessageRepository.save(failed);
            log.warn("⚠️ Saved failed message {} to DB for retry", event.id());
        } catch (Exception e) {
            log.error("❌ Error while saving failed message to DB for event {}", event.id(), e);
        }
    }


}
