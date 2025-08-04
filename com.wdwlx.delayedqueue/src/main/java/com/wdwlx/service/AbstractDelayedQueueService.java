package com.wdwlx.service;

import cn.hutool.core.collection.CollUtil;
import com.wdwlx.entity.DelayedMessage;
import com.wdwlx.util.IdManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.NonNull;
import org.redisson.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class AbstractDelayedQueueService {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDelayedQueueService.class);

    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private DelayedMessageService delayedMessageService;

    // 注入处理线程池
    @Autowired
    @Qualifier("delayedQueueProcessorExecutor")
    private ThreadPoolTaskExecutor processorExecutor;

    // 注入监听线程池
    @Autowired
    @Qualifier("delayedQueueListenerExecutor")
    private ScheduledExecutorService listenerExecutor;

    // 锁超时配置
    @Value("${delayed.queue.lock.wait-timeout-seconds:5}")
    private int lockWaitTimeoutSeconds;

    @Value("${delayed.queue.lock.lease-timeout-seconds:15}")
    private int lockLeaseTimeoutSeconds;

    @Autowired
    private IdManager idManager;

    private RDelayedQueue<String> delayedQueue;
    private RQueue<String> queue;
    private RBlockingQueue<String> blockingQueue;

    // 添加定时任务的Future引用
    private ScheduledFuture<?> listenerTaskFuture;
    private volatile boolean isListening = false;

    // 抽象方法，由子类提供队列名称
    protected abstract String getQueueName();

    // 抽象方法，由子类实现具体的消息处理逻辑
    protected abstract void handleMessage(DelayedMessage message) throws Exception;

    // 抽象方法，由子类定义是否需要处理积压消息
    protected abstract boolean shouldProcessBacklogMessages();

    // 抽象方法，由子类定义是否需要重复消息
    protected abstract boolean shouldRepeatedMessage();


    @PostConstruct
    public void init() {
        // 初始化队列
        String queueName = getQueueName();
        logger.info("初始化队列：{}", queueName);
        queue = redissonClient.getQueue(queueName);
        blockingQueue = redissonClient.getBlockingQueue(queueName);
        // 将普通队列绑定为延迟队列
        delayedQueue = redissonClient.getDelayedQueue(queue);

        // 启动消息监听器
        startMessageListener();

        // 恢复未处理消息
        recoverUnprocessedMessages();

        // 处理积压消息（如果需要）
        if (shouldProcessBacklogMessages()) {
            processBacklogMessages();
        }
    }

    /**
     * 启动消息监听器（使用共享监听线程池）
     */
    private void startMessageListener() {
        String queueName = getQueueName();

        // 提交周期性任务到共享监听线程池并保存Future
        listenerTaskFuture = listenerExecutor.scheduleWithFixedDelay(this::checkQueueMessages, 0, 100, // 固定100毫秒检查间隔
                TimeUnit.MILLISECONDS);

        isListening = true;
        logger.info("注册队列监听器: {}", queueName);
    }

    /**
     * 恢复未处理消息
     */
    private void recoverUnprocessedMessages() {
        // 获取未到期的未处理消息
        List<DelayedMessage> messages = delayedMessageService.findUnprocessedMessages();
        for (DelayedMessage message : messages) {
            // 检查消息是否已经存在于队列中
            if (!queue.contains(message.getMessageId()) && !delayedQueue.contains(message.getMessageId())) {
                long delay = Duration.between(LocalDateTime.now(), message.getExpireTime()).getSeconds();
                if (delay > 0) {
                    delayedQueue.offer(message.getMessageId(), delay, TimeUnit.SECONDS);
                    logger.info("恢复未处理消息，queue: {}, messageId: {}, delay: {}s", getQueueName(), message.getMessageId(), delay);
                } else {
                    // 如果已经过期，直接放入普通队列立即处理
                    queue.offer(message.getMessageId());
                    logger.info("恢复已过期消息，立即处理，queue: {}, messageId: {}", getQueueName(), message.getMessageId());
                }
            } else {
                logger.info("消息已存在于队列中，跳过恢复: {}", message.getMessageId());
            }
        }
    }

    /**
     * 检查队列中的消息（增强版）
     */
    private void checkQueueMessages() {
        // 避免在关闭过程中继续检查
        if (!isListening) {
            return;
        }

        try {
            // 使用带超时的poll避免阻塞
            String messageId = blockingQueue.poll(10, TimeUnit.MILLISECONDS);
            if (messageId != null) {
                // 检查线程池是否已关闭
                if (!processorExecutor.getThreadPoolExecutor().isShutdown()) {
                    // 提交到处理线程池
                    processorExecutor.submit(new MessageProcessorTask(messageId));
                } else {
                    logger.warn("处理线程池已关闭，丢弃消息: {}", messageId);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("检查队列消息被中断, queue: {}", getQueueName());
        } catch (Exception e) {
            logger.error("检查队列消息异常, queue: {}", getQueueName(), e);
        }
    }

    /**
     * 消息处理任务
     */
    private class MessageProcessorTask implements Runnable {
        private final String messageId;

        public MessageProcessorTask(String messageId) {
            this.messageId = messageId;
        }

        @Override
        public void run() {
            String lockName = String.format("delayed_queue_processor_lock:%s:%s", getQueueName(), messageId);
            RLock lock = redissonClient.getLock(lockName);
            boolean acquired = false;

            try {
                // 使用看门狗机制：不设置租约时间（传-1），Redisson会自动续期
                acquired = lock.tryLock(lockWaitTimeoutSeconds, -1, TimeUnit.SECONDS);
                if (acquired) {
                    processMessage(messageId);
                } else {
                    logger.warn("获取分布式锁超时, queue: {}, messageId: {}", getQueueName(), messageId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("获取分布式锁被中断, queue: {}, messageId: {}", getQueueName(), messageId);
            } catch (Exception e) {
                logger.error("获取分布式锁异常, queue: {}, messageId: {}", getQueueName(), messageId, e);
            } finally {
                // 确保只有持有锁的线程才能释放锁
                if (acquired && lock.isHeldByCurrentThread()) {
                    try {
                        lock.unlock();
                    } catch (Exception e) {
                        logger.warn("释放分布式锁异常, queue: {}, messageId: {}", getQueueName(), messageId, e);
                    }
                }
            }
        }


    }

    public String addDelayedMessage(String content, @NonNull LocalDateTime expireTime, String topic, String bizId) {
        // 不允许重复消息，则过滤数据
        if (!shouldRepeatedMessage()) {
            List list = delayedMessageService.findByBizId(bizId,topic);
            if (CollUtil.isNotEmpty(list)) {
                logger.warn("消息已存在，bizId: {}", bizId);
                return null;
            }
        }

        LocalDateTime now = LocalDateTime.now();
        if (expireTime.isBefore(now)) {
            logger.warn("消息已过期, queue: {}, expireTime: {}", queue, topic);
            return null;
        }
        String messageId = idManager.getId();

        long delay = Duration.between(now, expireTime).getSeconds();

        // 创建消息实体
        DelayedMessage message = new DelayedMessage(messageId, content, expireTime, topic, bizId);
        message.setStatus(0);
        // 保存到数据库
        delayedMessageService.save(message);

        boolean queueAdded = false;
        int retryCount = 3;

        // 添加重试机制
        while (retryCount > 0 && !queueAdded) {
            try {
                if (expireTime.isEqual(now)) {
                    logger.warn("消息到期，立即触发, queue: {}, expireTime: {}", queue, topic);
                    processMessage(messageId);
                    queueAdded = true;
                } else {
                    // 添加到延时队列
                    delayedQueue.offer(messageId, delay, TimeUnit.SECONDS);
                    queueAdded = true;
                    logger.info("添加延时消息成功，queue: {}, messageId: {}, delay: {} {}", getQueueName(), messageId, delay, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                retryCount--;
                logger.warn("添加消息到延时队列失败，剩余重试次数: {}, messageId: {}", retryCount, messageId, e);
                if (retryCount == 0) {
                    // 重试失败，回滚数据库操作
                    delayedMessageService.deleteByMessageId(messageId);
                    logger.error("添加消息到延时队列最终失败，已回滚数据库记录，messageId: {}", messageId, e);
                    return null;
                }

                try {
                    Thread.sleep(100); // 短暂等待后重试
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

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
        if (message.getStatus() == 1) {
            logger.info("消息已处理，queue: {}, messageId: {}, status: {}", getQueueName(), messageId, message.getStatus());
            return;
        }

        // 更新消息状态为处理中
        if (!delayedMessageService.updateStatus(messageId, 2)) {
            logger.warn("更新消息状态失败，queue: {}, messageId: {}", getQueueName(), messageId);
            return;
        }

        try {
            // 记录处理开始时间
            long processStartTime = System.currentTimeMillis();

            // 执行业务逻辑
            handleMessage(message);

            // 记录处理结束时间
            long processEndTime = System.currentTimeMillis();

            // 更新消息状态为已处理
            delayedMessageService.updateStatus(messageId, 1);
            logger.info("处理延时消息成功，queue: {}, messageId: {}, 处理耗时: {}ms",
                    getQueueName(), messageId, (processEndTime - processStartTime));
        } catch (Exception e) {
            // 处理失败，恢复状态为未处理，便于重试
            delayedMessageService.updateStatus(messageId, 0);
            logger.error("处理延时消息失败，queue: {}, messageId: {}", getQueueName(), messageId, e);
        }
    }

    /**
     * 处理积压消息（服务启动时调用）
     */
    private void processBacklogMessages() {
        List<DelayedMessage> messages = delayedMessageService.findPendingMessages(LocalDateTime.now());
        for (DelayedMessage message : messages) {
            // 检查消息是否已存在于队列中，避免重复添加
            if (!queue.contains(message.getMessageId()) && !delayedQueue.contains(message.getMessageId())) {
                queue.offer(message.getMessageId());
                logger.info("处理积压消息，queue: {}, messageId: {}", getQueueName(), message.getMessageId());
            } else {
                logger.info("积压消息已存在于队列中，跳过: {}", message.getMessageId());
            }
        }
    }

    @PreDestroy
    public void destroy() {
        // 取消定时任务
        if (listenerTaskFuture != null && !listenerTaskFuture.isCancelled()) {
            listenerTaskFuture.cancel(false);
        }

        isListening = false;
        logger.info("销毁队列监听器: {}", getQueueName());
    }


    /**
     * 检查服务健康状态
     */
    public boolean isHealthy() {
        return isListening &&
                !processorExecutor.getThreadPoolExecutor().isShutdown() &&
                !processorExecutor.getThreadPoolExecutor().isTerminated();
    }

    /**
     * 获取队列统计信息
     */
    public Map<String, Object> getQueueStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("queueName", getQueueName());
        stats.put("isListening", isListening);
        stats.put("processorActiveCount", processorExecutor.getActiveCount());
        stats.put("processorQueueSize", processorExecutor.getThreadPoolExecutor().getQueue().size());
        stats.put("blockingQueueSize", blockingQueue.size());
        stats.put("delayedQueueSize", delayedQueue.size());
        return stats;
    }
}
