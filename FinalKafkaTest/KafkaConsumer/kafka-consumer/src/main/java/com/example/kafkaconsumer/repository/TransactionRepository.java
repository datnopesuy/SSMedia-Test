package com.example.kafkaconsumer.repository;

import com.example.kafkaconsumer.entity.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, String> {
    boolean existsById(String id);
}
