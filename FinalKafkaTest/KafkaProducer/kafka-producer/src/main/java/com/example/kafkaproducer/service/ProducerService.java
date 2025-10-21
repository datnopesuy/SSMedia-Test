package com.example.kafkaproducer.service;

import com.example.kafkaproducer.dto.TransactionEvent;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
public class ProducerService {

    KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    private static final String TOPIC = "transaction-logs";
    private static final String DLQ_TOPIC = "transaction-logs-dlq";
    Random random = new Random();

    @Scheduled(fixedDelay = 6_000)
    public void sendTransactionEvent() {
        for (int i = 0; i < 100; i++) {
            try {
                TransactionEvent event = new TransactionEvent(
                        UUID.randomUUID().toString(),
                        LocalDateTime.now(),
                        "user-" + random.nextInt(1000),
                        BigDecimal.valueOf(random.nextDouble() * 1000)
                );
                sendWithRetry(event);
            } catch (Exception ex) {
                log.error("Unexpected error in iteration {}: {}", i, ex.getMessage());
            }
        }
    }

    @Retryable(
            value = {Exception.class},
            maxAttempts = 5,
            backoff = @Backoff(
                    delay = 1000,
                    multiplier = 2,
                    maxDelay = 10000
            )
    )
    public void sendWithRetry(TransactionEvent event) {
        ProducerRecord<String, TransactionEvent> record =
                new ProducerRecord<>(TOPIC, event.userId(), event);

        CompletableFuture<SendResult<String, TransactionEvent>> future =
                kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send message with key={}: {}", event.id(), ex.getMessage());
                throw new RuntimeException("Send failed", ex);
            } else {
                log.info("Sent to topic={}, key={}, partition={}, offset={}",
                        TOPIC, event.id(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        // Block để đảm bảo thứ tự gửi trong cùng một userId
        try {
            future.get();
        } catch (Exception e) {
            throw new RuntimeException("Send failed", e);
        }
    }

    @Recover
    public void recover(Exception e, TransactionEvent event) {
        log.warn("All retries exhausted for message {}, sending to DLQ", event.id());
        sendToDLQ(event);
    }

    private void sendToDLQ(TransactionEvent event) {
        try {
            kafkaTemplate.send(DLQ_TOPIC, event.userId(), event)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send message {} to DLQ: {}", event.id(), ex.getMessage());
                        } else {
                            log.warn("Moved message {} to DLQ after max retries", event.id());
                        }
                    });
        } catch (Exception ex) {
            log.error("Critical: Failed to send message {} to DLQ: {}", event.id(), ex.getMessage());
        }
    }
}