package com.example.kafkaconsumer.service;

import com.example.kafkaconsumer.entity.Transaction;
import com.example.kafkaconsumer.repository.TransactionRepository;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ConsumerService implements AcknowledgingMessageListener<String, Transaction> {

    TransactionRepository transactionRepository;
    DynamicThreadPoolService threadPoolService;

    @Override
    @KafkaListener(topics = {"transaction-logs"}, groupId = "transactions-listener-group", concurrency = "3")
    public void onMessage(ConsumerRecord<String, Transaction> data, Acknowledgment acknowledgment) {
        threadPoolService.submitTask(() -> {
            try {
                System.out.println("Received message: " + data.toString());
                Transaction transaction = data.value();
                if(!transactionRepository.existsById(transaction.getId())){
                    transactionRepository.save(transaction);
                }
                System.out.println("[" + Thread.currentThread().getName() + "] Successfully saved transaction with ID: " + transaction.getId());
                acknowledgment.acknowledge();
            } catch (Exception e) {
                System.err.println("Error processing message: " + e.getMessage());
                throw e;
            }});
    }
}
