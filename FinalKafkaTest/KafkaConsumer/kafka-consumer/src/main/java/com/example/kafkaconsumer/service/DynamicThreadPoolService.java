package com.example.kafkaconsumer.service;

import com.example.kafkaconsumer.repository.AppConfigRepository;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = false)
public class DynamicThreadPoolService {

    final AppConfigRepository appConfigRepository;
    private ThreadPoolTaskExecutor executor;
    int currentThreadCount = 0;

    public void init() {
        if (executor == null) {
            executor = new ThreadPoolTaskExecutor();
            executor.setThreadNamePrefix("txn-worker-");
            executor.setCorePoolSize(getThreadCountFromDb());
            executor.initialize();
            updateThreadPool(); // Lấy cấu hình ban đầu từ DB
        }
    }

    public void submitTask(Runnable task) {
        if (executor == null) init();
        executor.execute(task);
    }

    // Hàm này chạy định kỳ mỗi 10 giây để kiểm tra DB có thay đổi không
    @Scheduled(fixedRate = 10_000)
    public void updateThreadPool() {
        int newThreadCount = getThreadCountFromDb();
        if (newThreadCount != currentThreadCount) {
            currentThreadCount = newThreadCount;
            if (executor != null) {
                executor.setCorePoolSize(currentThreadCount);
                executor.setMaxPoolSize(currentThreadCount);
                System.out.println("Updated thread pool size to " + currentThreadCount);
            }
        }
    }

    private int getThreadCountFromDb() {
        return appConfigRepository.findByConfigKey("thread_count")
                .map(cfg -> Integer.parseInt(cfg.getConfigValue()))
                .orElse(5); // mặc định 5 luồng
    }
}
