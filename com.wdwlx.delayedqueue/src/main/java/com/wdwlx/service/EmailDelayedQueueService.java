package com.wdwlx.service;

import com.wdwlx.entity.DelayedMessage;
import com.wdwlx.service.AbstractDelayedQueueService;
import org.springframework.stereotype.Service;

@Service
public class EmailDelayedQueueService extends AbstractDelayedQueueService {

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
    protected String getQueueName() {
        return "email_delayed_queue";
    }

    @Override
    protected boolean repeatedMessage() {
        return false;
    }

    @Override
    protected void handleMessage(DelayedMessage message) throws Exception {
        // 实现邮件发送逻辑
        System.out.println("---------发送邮件---------: " + message.getContent());
        // 模拟处理时间
        Thread.sleep(100);
    }

    @Override
    protected boolean shouldProcessBacklogMessages() {
        return true;
    }
}
