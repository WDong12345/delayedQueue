package com.wdwlx.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class DelayedQueueThreadPoolConfig {

    // 处理线程池配置
    @Value("${delayed.queue.processor.thread.pool.core-size:10}")
    private int processorCorePoolSize;

    @Value("${delayed.queue.processor.thread.pool.max-size:20}")
    private int processorMaxPoolSize;

    @Value("${delayed.queue.processor.thread.keepalive-seconds:60}")
    private int processorKeepAliveSeconds;

    @Value("${delayed.queue.processor.thread.queue-capacity:1000}")
    private int processorQueueCapacity;

    // 监听线程池配置
    @Value("${delayed.queue.listener.thread.pool-size:5}")
    private int listenerThreadPoolSize;

    @Bean("delayedQueueProcessorExecutor")
    public ThreadPoolTaskExecutor delayedQueueProcessorExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(processorCorePoolSize);
        executor.setMaxPoolSize(processorMaxPoolSize);
        executor.setKeepAliveSeconds(processorKeepAliveSeconds);
        executor.setQueueCapacity(processorQueueCapacity);
        executor.setThreadNamePrefix("DelayedQueueProcessor-");
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

    @Bean("delayedQueueListenerExecutor")
    public ScheduledExecutorService delayedQueueListenerExecutor() {
        return Executors.newScheduledThreadPool(
                Math.max(1, Math.min(listenerThreadPoolSize, Runtime.getRuntime().availableProcessors())),
                new ThreadFactory() {
                    private final AtomicInteger threadNumber = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "DelayedQueueListener-" + threadNumber.getAndIncrement());
                        t.setDaemon(true);
                        return t;
                    }
                }
        );
    }
}
