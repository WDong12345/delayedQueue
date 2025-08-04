package com.wdwlx.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.wdwlx.entity.DelayedMessage;
import com.wdwlx.repository.DelayedMessageRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class DelayedMessageRepositoryImpl extends ServiceImpl<DelayedMessageRepository, DelayedMessage> implements DelayedMessageService {

    private final DelayedMessageRepository delayedMessageRepository;

    @Override
    public List findByBizId(String bizId, String topic) {
        return delayedMessageRepository.selectList(
                new LambdaQueryWrapper<DelayedMessage>()
                .eq(DelayedMessage::getBizId, bizId)
                .eq(DelayedMessage::getTopic, topic));
    }

    @Override
    public boolean deleteByMessageId(String messageId) {

        int delete = delayedMessageRepository.delete(new LambdaUpdateWrapper<DelayedMessage>().eq(DelayedMessage::getMessageId, messageId));
        return delete > 0;
    }

    public DelayedMessageRepositoryImpl(DelayedMessageRepository delayedMessageRepository) {
        this.delayedMessageRepository = delayedMessageRepository;
    }

    @Override
    public boolean save(DelayedMessage message) {
        return baseMapper.insert(message) > 0;
    }

    @Override
    public DelayedMessage findByMessageId(String messageId) {
        if (messageId == null) {
            return null;
        }
        return baseMapper.selectOne(new LambdaQueryWrapper<DelayedMessage>().eq(DelayedMessage::getMessageId, messageId)
                .last(" limit 1 "));
    }

    @Override
    public List<DelayedMessage> findUnprocessedMessages() {
        return delayedMessageRepository.selectList(new LambdaQueryWrapper<DelayedMessage>().eq(DelayedMessage::getStatus, 0));
    }

    @Override
    public boolean updateStatus(String messageId, int status) {
        baseMapper.update(new LambdaUpdateWrapper<DelayedMessage>().eq(DelayedMessage::getMessageId, messageId)
                .set(DelayedMessage::getStatus, status)
                .set(DelayedMessage::getProcessTime, LocalDateTime.now()));
        return true;
    }

    @Override
    public List<DelayedMessage> findPendingMessages(LocalDateTime beforeTime) {
        return baseMapper.selectList(new LambdaQueryWrapper<DelayedMessage>().eq(DelayedMessage::getStatus, 0));
    }
}
