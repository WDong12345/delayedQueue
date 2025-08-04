package com.wdwlx.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.wdwlx.entity.DelayedMessage;

import java.time.LocalDateTime;
import java.util.List;

public interface DelayedMessageService extends IService<DelayedMessage> {

    DelayedMessage findByMessageId(String messageId);

    List<DelayedMessage> findUnprocessedMessages();

    boolean updateStatus(String messageId, int status);

    List<DelayedMessage> findPendingMessages(LocalDateTime beforeTime);

    boolean deleteByMessageId(String messageId);
}
