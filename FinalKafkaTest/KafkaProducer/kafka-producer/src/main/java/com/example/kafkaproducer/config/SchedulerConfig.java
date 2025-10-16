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
        scheduler.setCorePoolSize
        scheduler.setMaxPoolSize(15);
        scheduler.setThreadNamePrefix("scheduler-task-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(30);  // TRƯỚC initialize()
        scheduler.initialize();  // Gọi CUỐI CÙNG

        System.out.println("✅ SchedulerConfig loaded! Pool size = " + scheduler.getPoolSize());
        taskRegistrar.setTaskScheduler(scheduler);
    }
}
