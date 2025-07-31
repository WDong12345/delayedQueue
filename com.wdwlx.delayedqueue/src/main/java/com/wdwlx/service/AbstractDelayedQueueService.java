package com.wdwlx.service;

import com.wdwlx.entity.DelayedMessage;
import jakarta.annotation.PostConstruct;
import org.redisson.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public abstract class AbstractDelayedQueueService {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDelayedQueueService.class);

    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private DelayedMessageService delayedMessageService;

    private RDelayedQueue<String> delayedQueue;
    private RQueue<String> queue;
    private RBlockingQueue<String> blockingQueue;

    // 抽象方法，由子类提供队列名称
    protected abstract String getQueueName();

    // 抽象方法，由子类实现具体的消息处理逻辑
    protected abstract void handleMessage(DelayedMessage message) throws Exception;

    // 抽象方法，由子类定义是否需要处理积压消息
    protected abstract boolean shouldProcessBacklogMessages();

    @PostConstruct
    public void init() {
        // 初始化队列
        String queueName = getQueueName();
        queue = redissonClient.getQueue(queueName);
        delayedQueue = redissonClient.getDelayedQueue(queue);
        blockingQueue = redissonClient.getBlockingQueue(queueName);

        // 启动消息处理器
        startMessageProcessor();

        // 恢复未处理消息
        recoverUnprocessedMessages();

        // 处理积压消息（如果需要）
        if (shouldProcessBacklogMessages()) {
            processBacklogMessages();
        }
    }

    /**
     * 启动消息处理器
     */
    private void startMessageProcessor() {
        Thread processorThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // 使用 poll 方法并设置超时时间，避免一直阻塞
                    String messageId = blockingQueue.poll(10, TimeUnit.SECONDS);

                    // 如果没有获取到消息，继续下一次循环
                    if (messageId == null) {
                        continue;
                    }

                    // 使用分布式锁防止重复处理
                    String lockName = "delayed_queue_processor_lock_" + getQueueName() + "_" + messageId;
                    RLock lock = redissonClient.getLock(lockName);
                    boolean acquired = lock.tryLock(10, 30, TimeUnit.SECONDS);

                    if (acquired) {
                        try {
                            processMessage(messageId);
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("处理延时消息异常, queue: {}", getQueueName(), e);
                    // 出现异常时短暂休眠，避免频繁重试导致CPU占用过高
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
        processorThread.setDaemon(true);
        processorThread.setName("DelayedMessageProcessor-" + getQueueName());
        processorThread.start();
    }

    /**
     * 添加延时消息
     *
     * @param content 消息内容
     * @param delay   延时时间
     * @param unit    时间单位
     * @param topic   消息主题
     * @return 消息ID
     */
    public String addDelayedMessage(String content, long delay, TimeUnit unit, String topic) {
        String messageId = UUID.randomUUID().toString();
        LocalDateTime processTime = LocalDateTime.now().plusSeconds(unit.toSeconds(delay));

        // 创建消息实体
        DelayedMessage message = new DelayedMessage(messageId, content, processTime, topic);
        message.setStatus(0);
        // 保存到数据库
        delayedMessageService.save(message);

        // 添加到延时队列
        delayedQueue.offer(messageId, delay, unit);

        logger.info("添加延时消息成功，queue: {}, messageId: {}, delay: {} {}",
                getQueueName(), messageId, delay, unit);
        return messageId;
    }

    /**
     * 处理消息
     *
     * @param messageId 消息ID
     */
    private void processMessage(String messageId) {
        // 从数据库获取消息详情
        DelayedMessage message = delayedMessageService.findByMessageId(messageId);
        if (message == null) {
            logger.warn("消息不存在，queue: {}, messageId: {}", getQueueName(), messageId);
            return;
        }

        // 检查消息状态，避免重复处理
        if (message.getStatus() != 0) {
            logger.info("消息已处理，queue: {}, messageId: {}, status: {}",
                    getQueueName(), messageId, message.getStatus());
            return;
        }

        // 更新消息状态为处理中
        if (!delayedMessageService.updateStatus(messageId, 2)) {
            logger.warn("更新消息状态失败，queue: {}, messageId: {}", getQueueName(), messageId);
            return;
        }

        try {
            // 执行业务逻辑
            handleMessage(message);

            // 更新消息状态为已处理
            delayedMessageService.updateStatus(messageId, 1);
            logger.info("处理延时消息成功，queue: {}, messageId: {}", getQueueName(), messageId);
        } catch (Exception e) {
            // 处理失败，恢复状态为未处理，便于重试
            delayedMessageService.updateStatus(messageId, 0);
            logger.error("处理延时消息失败，queue: {}, messageId: {}", getQueueName(), messageId, e);
        }
    }

    /**
     * 恢复未处理消息
     */
    private void recoverUnprocessedMessages() {
        // 获取未到期的未处理消息
        List<DelayedMessage> messages = delayedMessageService.findUnprocessedMessages();
        for (DelayedMessage message : messages) {
            long delay = Duration.between(LocalDateTime.now(), message.getProcessTime()).getSeconds();
            if (delay > 0) {
                delayedQueue.offer(message.getMessageId(), delay, TimeUnit.SECONDS);
                logger.info("恢复未处理消息，queue: {}, messageId: {}, delay: {}s",
                        getQueueName(), message.getMessageId(), delay);
            }
        }
    }

    /**
     * 处理积压消息（服务启动时调用）
     */
    private void processBacklogMessages() {
        List<DelayedMessage> messages = delayedMessageService.findPendingMessages(LocalDateTime.now());
        for (DelayedMessage message : messages) {
            // 直接放入队列立即处理
            queue.offer(message.getMessageId());
            logger.info("处理积压消息，queue: {}, messageId: {}", getQueueName(), message.getMessageId());
        }
    }

    // 提供获取队列实例的方法，供子类使用
    protected RDelayedQueue<String> getDelayedQueue() {
        return delayedQueue;
    }

    protected RQueue<String> getQueue() {
        return queue;
    }

    protected RBlockingQueue<String> getBlockingQueue() {
        return blockingQueue;
    }

    protected RedissonClient getRedissonClient() {
        return redissonClient;
    }

    protected DelayedMessageService getRepository() {
        return delayedMessageService;
    }
}
