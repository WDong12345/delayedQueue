// #file:F:\工作目录\java_wp\com.wdwlx.delayedqueue\src\main\java\com\wdwlx\service\NotificationDelayedQueueService.java
package com.wdwlx.service;

import com.wdwlx.entity.DelayedMessage;
import org.springframework.stereotype.Service;

@Service
public class NotificationDelayedQueueService extends AbstractDelayedQueueService {
    
    @Override
    protected String getQueueName() {
        return "notification_delayed_queue";
    }
    
    @Override
    protected void handleMessage(DelayedMessage message) throws Exception {
        // 实现通知相关的延时处理逻辑
        System.out.println("---------发送延时通知---------: " + message.getContent());
        // 例如：发送邮件、短信、站内信等
        // 这里可以调用通知服务的相关方法
        
        // 模拟业务处理时间
        Thread.sleep(50);
    }
    
    @Override
    protected boolean shouldProcessBacklogMessages() {
        return true;
    }
}
