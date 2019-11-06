package com.jarvis.rocketmq.jms;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OrderPayConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrderPayConsumer.class);

    private DefaultMQPushConsumer consumer;

    private String consumerGroup = "order_pay_consumer_group";

    public OrderPayConsumer() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(JMSConfig.NAME_SERVER);
        consumer.setVipChannelEnabled(false);
        //默认是集群方式，可以更改为广播，但是不支持重试
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        /*设置消费策略，从最末节点消费 默认是CONSUME_FROM_LAST_OFFSET*/
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        /*订阅主题*/
        consumer.subscribe(JMSConfig.ORDER_TEST_TOPIC, "*");
        /*注册监听器，一有消息过来就触发里边的方法*/
        /**
         * 消息最多重试16次（默认）
         * 超过就人工补偿,死讯队列
         * 不再广播方式生效
         *
         * 注意的是顺序消费的时候需要做的是保证消费同个topic下的同个队列，不应该用MessageListenerConcurrently--->多线程消费
         * 应该使用MessageListenOrderly，自带单线程消费消息，不能用consumer多线程去消费，消费端分配到的queue是固定的
         * 集群会所著当前正在消费的队列集合的消息，所以保证顺序消费
         * */
        /*
        * 下边例子是为了顺序消费用的
        * context.setAutoCommit();只有在顺序执行才有
        * 多个消费端默认是通过轮询消费，顺序是通过在队列上枷锁实现
        * 为每个consumerQueue加锁，消费每个消息前，都需要获取这个消息所在的queue的锁，同个时间，同个queue不会被兵法消费，但是不同的queue可以并发处理
        * 类似ConcurrentHashMap的原理，桶锁segment
        * */
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                logger.info("收到的是{},线程{}，重试次数{}", new String(msgs.get(0).getBody()),Thread.currentThread().getName(), msgs.get(0).getReconsumeTimes());
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
    }
}
