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
    ObjectMapper objectMapper = new ObjectMapper();

    @Scheduled(fixedDelay = 60_000)
    public void retryFailedMessage() {
        List<FailedMessage> failedMessages = failedMessageRepository.findTop100ByOrderByCreatedAtAsc();

        for (FailedMessage failedMessage : failedMessages) {
            try {
                TransactionEvent event = objectMapper.readValue(failedMessage.getPayloadJson(), TransactionEvent.class);
                kafkaTemplate.send(failedMessage.getTopicName(), event.id(), event).get();
                log.info("Retried and successfully sent message {}", failedMessage.getId());
                failedMessageRepository.delete(failedMessage);
            }catch (Exception e) {
                failedMessage.setRetryCount(failedMessage.getRetryCount() + 1);
                failedMessage.setErrorMessage(e.getMessage());
                failedMessageRepository.save(failedMessage);
                log.warn("Retry failed for message {} (count={})", failedMessage.getId(), failedMessage.getRetryCount());
            }
        }
    }
}
