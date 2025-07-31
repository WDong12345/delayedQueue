package com.wdwlx.repository;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.wdwlx.entity.DelayedMessage;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface DelayedMessageRepository extends BaseMapper<DelayedMessage> {

}
