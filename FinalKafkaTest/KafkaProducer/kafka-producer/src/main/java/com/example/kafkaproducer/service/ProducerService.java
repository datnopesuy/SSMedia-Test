package com.example.kafkaproducer.service;

import com.example.kafkaproducer.dto.TransactionEvent;
import com.example.kafkaproducer.entity.FailedMessage;
import com.example.kafkaproducer.repository.FailedMessageRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
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
    private static final String DLQ_TOPIC = "transaction-logs-dlq";
    private static final int MAX_RETRIES = 5;
    Random random = new Random();

    @Scheduled(fixedDelay = 6_000)
    public void sendTransactionEvent() {
        for (int i = 0; i < 100 ; i++) {
            try {
                TransactionEvent event = new TransactionEvent(
                        UUID.randomUUID().toString(),
                        LocalDateTime.now(),
                        "user-" + random.nextInt(100),
                        BigDecimal.valueOf(random.nextDouble() * 1000)
                );
                sendWithRetry(event);
            }catch (Exception ex) {
                log.error("Unexpected error in iteration {}: {}", i, ex.getMessage());
            }
        }
    }

    public void sendWithRetry(TransactionEvent event) {
        var record = createProducerRecord(event);

        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            try {
                kafkaTemplate.send(record).get();
                log.info("Sent to topic={}, key={}, partition={}",
                        TOPIC, record.key(), record.partition());
                return;
            }catch (Exception ex) {
                log.warn("Retry {}/{} failed for key={} : {}", retry + 1, MAX_RETRIES, record.key(), ex.getMessage());

                if(retry < MAX_RETRIES - 1) {
                    try {
                        Thread.sleep(1000 * (retry + 1));
                    }catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Retry interrupted for key={}", event.id());
                        break;
                    }
                }
            }
        }
        sendToDLQ(event);
    }

    private void sendToDLQ(TransactionEvent event) {
        try {
            kafkaTemplate.send(DLQ_TOPIC, event.id(), event).get();
            log.warn("Moved message {} to DLQ after {} retries", event.id(), MAX_RETRIES);
        }catch (Exception ex) {
            log.error("Failed to send message {} to DLQ: {}", event.id(), ex.getMessage());
        }
    }

    public ProducerRecord<String, TransactionEvent> createProducerRecord(TransactionEvent transactionEvent) {
        return new ProducerRecord<>(TOPIC, transactionEvent.id(), transactionEvent);
    }


}
