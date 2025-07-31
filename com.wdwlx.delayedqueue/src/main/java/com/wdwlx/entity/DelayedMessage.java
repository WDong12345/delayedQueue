package com.wdwlx.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DelayedMessage {
    private Long id;
    private String messageId;
    private String content;
    private LocalDateTime createTime = LocalDateTime.now();
    private LocalDateTime processTime;
    private Integer status; // 0:未处理, 1:已处理, 2:处理中
    private String topic;

    public DelayedMessage(String messageId, String content, LocalDateTime processTime, String topic) {
        this.messageId = messageId;
        this.content = content;
        this.processTime = processTime;
        this.topic = topic;
    }
}
