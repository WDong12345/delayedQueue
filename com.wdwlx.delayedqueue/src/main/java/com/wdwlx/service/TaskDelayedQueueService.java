// #file:F:\工作目录\java_wp\com.wdwlx.delayedqueue\src\main\java\com\wdwlx\service\TaskDelayedQueueService.java
package com.wdwlx.service;

import com.wdwlx.entity.DelayedMessage;
import org.springframework.stereotype.Service;

@Service
public class TaskDelayedQueueService extends AbstractDelayedQueueService {
    
    @Override
    protected String getQueueName() {
        return "task_delayed_queue";
    }
    
    @Override
    protected void handleMessage(DelayedMessage message) throws Exception {
        // 实现任务相关的延时处理逻辑
        System.out.println("---------执行延时任务---------: " + message.getContent());
        // 例如：定时执行某些任务、清理过期数据等
        // 这里可以调用任务服务的相关方法
        
        // 模拟业务处理时间
        Thread.sleep(200);
    }
    
    @Override
    protected boolean shouldProcessBacklogMessages() {
        return false; // 任务队列不需要处理积压消息
    }
}
