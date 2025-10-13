package com.example.kafkaconsumer.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "app_config")
public class AppConfig {

    @Id
    private Long id;
    private String configKey;
    private String configValue;

}
