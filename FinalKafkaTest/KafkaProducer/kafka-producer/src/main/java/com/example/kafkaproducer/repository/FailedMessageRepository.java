package com.example.kafkaproducer.repository;

import com.example.kafkaproducer.entity.FailedMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface FailedMessageRepository extends JpaRepository<FailedMessage, String> {
    List<FailedMessage> findTop10ByOrderByCreatedAtAsc();
}
