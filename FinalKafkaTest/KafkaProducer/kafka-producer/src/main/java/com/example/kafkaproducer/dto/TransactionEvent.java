package com.example.kafkaproducer.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record TransactionEvent(
        String id,
        LocalDateTime timestamp,
        String userId,
        BigDecimal amount
) {
}
