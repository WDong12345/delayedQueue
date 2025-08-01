// #file:F:\工作目录\java_wp\com.wdwlx.delayedqueue\src\main\java\com\wdwlx\service\OrderDelayedQueueService.java
package com.wdwlx.service;

import com.wdwlx.entity.DelayedMessage;
import org.springframework.stereotype.Service;

@Service
public class OrderDelayedQueueService extends AbstractDelayedQueueService {
    
    @Override
    protected String getQueueName() {
        return "order_delayed_queue";
    }
    
    @Override
    protected void handleMessage(DelayedMessage message) throws Exception {
        // 实现订单相关的延时处理逻辑
        System.out.println("---------处理订单延时消息---------: " + message.getContent());
        // 例如：自动取消超时未支付订单、自动确认收货等
        // 这里可以调用订单服务的相关方法
        
        // 模拟业务处理时间
        Thread.sleep(100);
    }
    
    @Override
    protected boolean shouldProcessBacklogMessages() {
        return true;
    }
}
