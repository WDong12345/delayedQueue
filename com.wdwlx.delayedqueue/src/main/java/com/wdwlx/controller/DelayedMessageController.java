package com.wdwlx.controller;

import com.wdwlx.service.NotificationDelayedQueueService;
import com.wdwlx.service.OrderDelayedQueueService;
import com.wdwlx.service.TaskDelayedQueueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

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
    public String sendOrderDelayedMessage(@RequestParam String content, @RequestParam String topic, @RequestParam String expireTimeStr) {

        LocalDateTime expireTime = LocalDateTime.parse(expireTimeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        if (Objects.equals(topic, "order")) {
            return orderDelayedQueueService.addDelayedMessage(content, expireTime, topic);
        }
        if (Objects.equals(topic, "task")) {
            return taskDelayedQueueService.addDelayedMessage(content, expireTime, topic);
        }
        if (Objects.equals(topic, "notification")) {
            return notificationDelayedQueueService.addDelayedMessage(content, expireTime, topic);
        }
        return "   异常";
    }

    @PostMapping("/addBatch")
    public String sendOrderDelayedMessageBatch() {
        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < 200; i++) {
            LocalDateTime expireTime = now.plusSeconds(i);
            orderDelayedQueueService.addDelayedMessage("order" + i, expireTime, "order");
            taskDelayedQueueService.addDelayedMessage("task" + i, expireTime, "task");
            notificationDelayedQueueService.addDelayedMessage("notification" + i, expireTime, "notification");
        }
        return "";

    }


}
