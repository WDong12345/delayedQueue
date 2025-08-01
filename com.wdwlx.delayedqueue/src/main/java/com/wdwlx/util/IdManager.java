package com.wdwlx.util;


import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;


/**
 * 分布式ID生产
 *
 * @author ZhangLi
 * @date 2024/03/14 15:22
 **/
@Slf4j
@Component("mqIdManager")
public class IdManager {

    @Resource(name = "stringRedisTemplate")
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private Environment environment;

    private SnowflakeIdWorker snowflakeIdWorker;

    @PostConstruct
    public void init() {
        String applicationName = environment.getProperty("spring.application.name");

        Long increment = stringRedisTemplate.opsForValue().increment("snowflake:workerId-" + applicationName);

        long workerId = increment & 0x000003FF;
        log.info("IdManagerImpl.init snowflake worker id is {}", workerId);

        snowflakeIdWorker = new SnowflakeIdWorker(workerId);
    }

    public String getId() {
        long nextId = snowflakeIdWorker.nextId();
        return Long.toString(nextId);
    }
}
