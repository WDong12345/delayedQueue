// #file:F:\工作目录\java_wp\com.wdwlx.delayedqueue\src\main\java\com\wdwlx\controller\DelayedMessageController.java
package com.wdwlx.controller;

import com.wdwlx.service.NotificationDelayedQueueService;
import com.wdwlx.service.OrderDelayedQueueService;
import com.wdwlx.service.TaskDelayedQueueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/delayed")
public class DelayedMessageController {

    @Autowired
    private OrderDelayedQueueService orderDelayedQueueService;

    @Autowired
    private NotificationDelayedQueueService notificationDelayedQueueService;

    @Autowired
    private TaskDelayedQueueService taskDelayedQueueService;

    @PostMapping("/add")
    public String sendOrderDelayedMessage(
            @RequestParam String content,
            @RequestParam String topic,
            @RequestParam long delay,
            @RequestParam TimeUnit unit) {
        if (Objects.equals(topic, "order")) {
            return orderDelayedQueueService.addDelayedMessage(content, delay, unit, topic);
        }
        if (Objects.equals(topic, "task")) {
            return taskDelayedQueueService.addDelayedMessage(content, delay, unit, topic);
        }
        if (Objects.equals(topic, "notification")) {
            return notificationDelayedQueueService.addDelayedMessage(content, delay, unit, topic);
        }

        return "   异常";
    }

}
