package com.wdwlx.service;

import com.wdwlx.entity.DelayedMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TaskDelayedQueueService extends AbstractDelayedQueueService {

    @Override
    protected String getQueueName() {
        return "task_delayed_queue";
    }

    @Override
    protected long getCheckInterval() {
        return 100;
    }

    @Override
    protected long getBloomFilterSize() {
        // 初始化bloom filter大小容量，可以根据实际情况调整
        return 100_000;
    }

    @Override
    protected boolean repeatedMessage() {
        return false;
    }

    @Override
    protected void handleMessage(DelayedMessage message) throws Exception {
        // 实现任务相关的延时处理逻辑
        log.info("---------执行延时任务---------: " + message.getContent());
        // 例如：定时执行某些任务、清理过期数据等
        // 这里可以调用任务服务的相关方法

        // 模拟业务处理时间
//         Thread.sleep(200);
    }

    @Override
    protected boolean shouldProcessBacklogMessages() {
        return false; // 任务队列不需要处理积压消息
    }
}
