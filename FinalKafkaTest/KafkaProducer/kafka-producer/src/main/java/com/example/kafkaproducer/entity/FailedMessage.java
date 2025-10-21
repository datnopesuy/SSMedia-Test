//package com.example.kafkaproducer.entity;
//
//import jakarta.persistence.Entity;
//import jakarta.persistence.Id;
//import jakarta.persistence.Table;
//import lombok.AllArgsConstructor;
//import lombok.Builder;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//
//import java.time.LocalDateTime;
//
//@Entity
//@Table(name = "failed_messages")
//@Data
//@NoArgsConstructor
//@AllArgsConstructor
//@Builder
//public class FailedMessage {
//
//    @Id
//    String id;
//
//    String topicName;
//
//    String keyValue;
//
//    String payloadJson;
//
//    String errorMessage;
//
//    int retryCount;
//
//    LocalDateTime createdAt;
//
//}
