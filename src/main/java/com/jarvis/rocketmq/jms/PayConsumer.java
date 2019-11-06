package com.jarvis.rocketmq.jms;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class PayConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PayConsumer.class);

    private DefaultMQPushConsumer consumer;

    private String consumerGroup = "pay_consumer_group";

    public PayConsumer() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(JMSConfig.NAME_SERVER);
        consumer.setVipChannelEnabled(false);
        //默认是集群方式，可以更改为广播，但是不支持重试
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        /*设置消费策略，从最末节点消费 默认是CONSUME_FROM_LAST_OFFSET ，跳过历史的
        * CONSUME_FROM_FIRST_OFFSET 从对头开始消费
        * CONSUME_FROM_TIMESTAMP 通过时间点消费的
        * 这里设置了消费策略都是只会遵循第一次消费方式做的
        * 以后就算修改了也不生效，但是有个问题是假如是不同group的话都是会从新历史的队列消费
        * */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        /**
         * 选取consumner的负载算法，多个consumenr的时候分配队列
         * 默认是平均分配的AllocateMessageQueueAveragely
         *
         * */
        consumer.setAllocateMessageQueueStrategy(new AllocateMessageQueueAveragely());

        /**
         * offsetStore消费进度存储器，假如是广播的默认是localFileOffsetStore本地存储，假如是集群的就是RemoteBrokerOffsetStore在服务器上记录
         * pullBatchSize 配置批量拉去多少个消息 默认32条
         * */
        /*订阅主题*/
        consumer.subscribe(JMSConfig.TOPIC, "*");
        /*注册监听器，一有消息过来就触发里边的方法*/
        /**
         * 消息最多重试16次（默认）
         * 超过就人工补偿,死讯队列
         * 不再广播方式生效
         *
         * 注意的是顺序消费的时候需要做的是保证消费同个topic下的同个队列，不应该用MessageListenerConcurrently--->多线程消费
         * 应该使用MessageListenOrderly，自带单线程消费消息，不能用consumer多线程去消费，消费端分配到的queue是固定的
         * 集群会所著当前正在消费的队列集合的消息，所以保证顺序消费
         * 尽量队列数多于消费者的数量，平均分配
         * */
        consumer.registerMessageListener((MessageListenerConcurrently)(msgs, context)->{
            MessageExt msg = msgs.get(0);
            logger.info("重传次数：{}", msg.getReconsumeTimes());
//            logger.info("body:{}", JSON.toJSONString(msg));
            logger.info("{}, Receive New Message:{}", Thread.currentThread().getName(), new String(msg.getBody()));
            String topic = msg.getTopic();
            String body = new String(msg.getBody());
            String tags = msg.getTags();
            String key = msg.getKeys();
            logger.info("topic:{}, tags={}, keys= {}, msg={}", topic, tags, key, body);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
    }
}
