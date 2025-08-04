package com.wdwlx.controller;

import cn.hutool.extra.spring.SpringUtil;
import com.wdwlx.service.AbstractDelayedQueueService;
import com.wdwlx.service.NotificationDelayedQueueService;
import com.wdwlx.service.OrderDelayedQueueService;
import com.wdwlx.service.TaskDelayedQueueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

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
        for (int i = 0; i < 10; i++) {
            LocalDateTime expireTime = now.plusSeconds(i);
            orderDelayedQueueService.addDelayedMessage("order" + i, expireTime, "order");
            taskDelayedQueueService.addDelayedMessage("task" + i, expireTime, "task");
            notificationDelayedQueueService.addDelayedMessage("notification" + i, expireTime, "notification");
        }
        return "";

    }

    @GetMapping("/health")
    public String health() {
        return "OK";
    }

    @GetMapping("/allStats")
    public List<Map<String, Object>> getAllQueueStats() {
        try {
            // 获取所有 AbstractDelayedQueueService 类型的 bean
            Map<String, AbstractDelayedQueueService> services = SpringUtil.getApplicationContext()
                    .getBeansOfType(AbstractDelayedQueueService.class);

            List<Map<String, Object>> allStats = new ArrayList<>();

            // 遍历所有服务实例并收集统计信息
            for (AbstractDelayedQueueService service : services.values()) {
                Map<String, Object> stats = service.getQueueStats();
                allStats.add(stats);
            }
            return allStats;
        } catch (Exception e) {
            // 记录异常日志
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

}
