package com.wdwlx.service;

import com.wdwlx.entity.DelayedMessage;
import com.wdwlx.service.AbstractDelayedQueueService;
import org.springframework.stereotype.Service;

@Service
public class EmailDelayedQueueService extends AbstractDelayedQueueService {

    @Override
    protected String getQueueName() {
        return "email_delayed_queue";
    }

    @Override
    protected boolean shouldRepeatedMessage() {
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
