package com.example.kafkaproducer.service;

import com.example.kafkaproducer.dto.TransactionEvent;
import com.example.kafkaproducer.entity.FailedMessage;
import com.example.kafkaproducer.repository.FailedMessageRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class FailedMessageRetryService {
    FailedMessageRepository failedMessageRepository;
    KafkaTemplate<String, TransactionEvent> kafkaTemplate;
    ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 60_000)
    public void retryFailedMessage() {
        List<FailedMessage> failedMessages = failedMessageRepository.findTop100ByOrderByCreatedAtAsc();

        for (FailedMessage failedMessage : failedMessages) {
            try {
                if(failedMessage.getRetryCount() >= 5) {
                    // Đã vượt quá retry limit
                    TransactionEvent event = objectMapper.readValue(failedMessage.getPayloadJson(), TransactionEvent.class);

                    try {
                        kafkaTemplate.send("transaction-logs-dlq", event.id(), event).get();
                        log.warn("Moved message {} to DLQ after {} retries", failedMessage.getId(), failedMessage.getRetryCount());
                        failedMessageRepository.delete(failedMessage);
                    } catch (Exception dlqEx) {
                        // QUAN TRỌNG: Nếu gửi DLQ thất bại, KHÔNG tăng retryCount nữa
                        failedMessage.setErrorMessage("Failed to send to DLQ: " + dlqEx.getMessage());
                        failedMessageRepository.save(failedMessage);
                        log.error("Failed to send message {} to DLQ: {}", failedMessage.getId(), dlqEx.getMessage());
                    }
                    continue; // Bỏ qua phần retry thường
                }

                // Retry bình thường (retryCount <= 5)
                TransactionEvent event = objectMapper.readValue(failedMessage.getPayloadJson(), TransactionEvent.class);
                kafkaTemplate.send(failedMessage.getTopicName(), event.id(), event).get();
                log.info("Retried and successfully sent message {}", failedMessage.getId());
                failedMessageRepository.delete(failedMessage);

            } catch (Exception e) {
                // Chỉ tăng retryCount nếu đang trong giai đoạn retry thường
                if (failedMessage.getRetryCount() < 5) {
                    failedMessage.setRetryCount(failedMessage.getRetryCount() + 1);
                    failedMessage.setErrorMessage(e.getMessage());
                    failedMessageRepository.save(failedMessage);
                    log.warn("Retry failed for message {} (count={})", failedMessage.getId(), failedMessage.getRetryCount());
                }
            }
        }
    }
}
