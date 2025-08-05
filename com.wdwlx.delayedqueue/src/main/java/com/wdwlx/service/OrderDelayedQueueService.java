package com.wdwlx.service;

import com.wdwlx.entity.DelayedMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderDelayedQueueService extends AbstractDelayedQueueService {

    @Override
    protected String getQueueName() {
        return "order_delayed_queue";
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
    protected void handleMessage(DelayedMessage message) throws Exception {
        // 实现订单相关的延时处理逻辑
        log.info("---------处理订单延时消息---------: " + message.getContent());
        // 例如：自动取消超时未支付订单、自动确认收货等
        // 这里可以调用订单服务的相关方法

        // 模拟业务处理时间
        Thread.sleep(100);
    }

    @Override
    protected boolean repeatedMessage() {
        return true;
    }

    @Override
    protected boolean shouldProcessBacklogMessages() {
        return true;
    }
}
