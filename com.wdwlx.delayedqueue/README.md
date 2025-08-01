
并发问题？  线程安全？    性能优化？   同一redis实例，应用服务为多个实例？




初始化队列
    AbstractDelayedQueueService -> init

    监听延时队列使用线程池  sharedThreadPool，那怕队列类型有100个也不会创建100个线程监听，以减少资源浪费

    poll到消息到使用线程池  sharedListenerPool
        




添加到队列



扩展性


到期处理



异常处理



消息堆积



