package com.jarvis.rocketmq.jms;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Component
public class PayProducer {
    /**
     * 定义生产者的group
     *
     */
    private String producerGroup = "pay_group";

    private DefaultMQProducer defaultMQProducer;

    public PayProducer() {
        //讲group传入创建实例
        defaultMQProducer = new DefaultMQProducer(producerGroup);
        //多节点、集群用;分隔
        defaultMQProducer.setNamesrvAddr(JMSConfig.NAME_SERVER);

        this.start();
    }

    /**
     *  对象在使用之前必须点用一次，只能初始化一次
     * */
    public void start(){
        try {
            this.defaultMQProducer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
    /**
     * 一般在应用上下文，使用上下文监听器，进行关闭
     * */
    public void shutDown(){
        this.defaultMQProducer.shutdown();
    }

    public DefaultMQProducer getProducer(){
        return this.defaultMQProducer;
    }
}
