package com.example.kafkaconsumer.service;

import com.example.kafkaconsumer.entity.Transaction;
import com.example.kafkaconsumer.repository.TransactionRepository;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
public class ConsumerService implements AcknowledgingMessageListener<String, Transaction> {

    TransactionRepository transactionRepository;
    DynamicThreadPoolService threadPoolService;

    @Override
    @RetryableTopic(
            attempts = "5",
            backoff = @Backoff(
                    delay = 1000,
                    multiplier = 2,
                    maxDelay = 10000
            ),
            autoCreateTopics = "true",
            include = {DataAccessException.class},
            dltTopicSuffix = "-consumer-dlq02"
    )
    @KafkaListener(topics = {"transaction-logs"}, groupId = "transactions-listener-group", concurrency = "3")
    public void onMessage(ConsumerRecord<String, Transaction> data, Acknowledgment acknowledgment) {
        threadPoolService.submitTask(() -> {
            try {
                Thread.sleep(2000);
                System.out.println("Waiting for 2 seconds");
                System.out.println("Received message: " + data.toString());
                Transaction transaction = data.value();

                if(!transactionRepository.existsById(transaction.getId())){
                    transactionRepository.save(transaction);
                }

                System.out.println("[" + Thread.currentThread().getName() + "] Successfully saved transaction with ID: " + transaction.getId());
                acknowledgment.acknowledge();
            } catch (Exception e) {
                System.err.println("Error processing message: " + e.getMessage());
            }});
    }

    @DltHandler
    public void handleDlt(Transaction transaction, @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exception) {
        log.error("Message moved to DLQ: {}, error: {}", transaction.getId(), exception);
    }
}
