package com.example.kafkaproducer.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
public class SchedulerConfig implements SchedulingConfigurer {
    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(15);
        scheduler.setThreadNamePrefix("Scheduler-task-");
        scheduler.initialize();
        scheduler.setAwaitTerminationSeconds(30);
        taskRegistrar.setTaskScheduler(scheduler);
    }
}
