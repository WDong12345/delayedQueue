## 基于Redis的延时队列，实现秒级延迟消息队列，自定义队列类型和延时时长


## 高并发下的主要保障机制

1. **分布式锁机制**
    - 使用 Redisson 的分布式锁 `RLock` 来确保同一消息不会被多个节点同时处理
    - 锁名称格式: `delayed_queue_processor_lock:{queueName}:{messageId}`，保证唯一性
    - 设置了合理的超时时间避免死锁

2. **线程池隔离**
    - 分离了监听线程池 ([listenerExecutor](listenerExecutor) 和处理线程池 ([processorExecutor](processorExecutor))
    - 避免监听线程被处理任务阻塞

3. **消息状态管理**
    - 数据库层面维护消息状态 (0-未处理, 1-已处理, 2-处理中)
    - 处理前检查状态避免重复处理



## 扩展性：
    extends AbstractDelayedQueueService即可，自定义队列类型以及到期逻辑。 参数 [OrderDelayedQueueService.java](src%2Fmain%2Fjava%2Fcom%2Fwdwlx%2Fservice%2FOrderDelayedQueueService.java)


## 队列数据存储：
1. **Redis**: 使用 Redis 的有序集合 (Sorted Set) 来存储消息，有序集合的 score 表示消息的到期时间。
2. **Mysql**: 本代码中redis只存储消息ID，真实消息内容存储在mysql中。实际使用中可根据业务需求选择调整存。
   下面为数据库表结构：
   ```mysql
   CREATE TABLE `delayed_message` (
     `id` bigint NOT NULL AUTO_INCREMENT,
     `message_id` varchar(64) NOT NULL COMMENT '消息唯一标识',
     `content` text NOT NULL COMMENT '消息内容',
     `create_time` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
     `process_time` datetime DEFAULT NULL COMMENT '处理时间',
     `status` tinyint NOT NULL DEFAULT '0' COMMENT '状态：0-未处理，1-已处理，2-处理中',
     `topic` varchar(100) NOT NULL COMMENT '消息主题',
     `expire_time` datetime NOT NULL COMMENT '过期时间',
     PRIMARY KEY (`id`),
     UNIQUE KEY `uk_message_id` (`message_id`),
     KEY `idx_process_time` (`process_time`),
     KEY `idx_status_process_time` (`status`,`process_time`)
   ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
   ```
 
