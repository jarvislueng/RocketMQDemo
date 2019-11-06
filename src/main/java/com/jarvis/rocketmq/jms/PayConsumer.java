package com.jarvis.rocketmq.jms;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;

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
        /*设置消费策略，从最末节点消费 默认是CONSUME_FROM_LAST_OFFSET*/
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        /*订阅主题*/
        consumer.subscribe(JMSConfig.TOPIC, "*");
        /*注册监听器，一有消息过来就触发里边的方法*/
        /**
         * 消息最多重试16次（默认）
         * 超过就人工补偿,死讯队列
         * 不再广播方式生效
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
