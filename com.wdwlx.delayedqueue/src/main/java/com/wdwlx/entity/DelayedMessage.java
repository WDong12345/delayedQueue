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
    /**
     * 业务ID,用于去重
     */
    private String bizId;
    private String content;
    private LocalDateTime createTime = LocalDateTime.now();
    private LocalDateTime processTime;
    /**
     * 过期时间
     */
    private LocalDateTime expireTime;
    private Integer status; // 0:未处理, 1:已处理, 2:处理中
    private String topic;

    public DelayedMessage(String messageId, String content, LocalDateTime expireTime, String topic, String bizId) {
        this.messageId = messageId;
        this.content = content;
        this.expireTime = expireTime;
        this.topic = topic;
        this.bizId = bizId;
    }
}
