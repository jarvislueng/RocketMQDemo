package com.jarvis.rocketmq.controller;

import com.jarvis.rocketmq.jms.JMSConfig;
import com.jarvis.rocketmq.jms.PayProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PayController {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private PayProducer payProducer;
    /*默认不会自动创建topic， 需要手动自行创建,或者修改broker的配置，可以自动创建*/
//    private static final String topic = "jarvis_pay_test_topic";
    /**
     * 出现了sendDefaultImpl call timeout的异常有可能是多网卡的问题（内网和公网）
     * 需要在broker中配置broker.conf配置公网网卡ip
     * */

    @RequestMapping("/api/v1/pay_cub")
    public Object callback() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        //key的话相当于id
        Message message = new Message(JMSConfig.TOPIC, "a", "hello jarvis22".getBytes());
        message.setDelayTimeLevel(2);
//        SendResult sendResult = payProducer.getProducer().send(message);//同步发送

        //里边的arg对应的是外层的arg
        SendResult sendResult = payProducer.getProducer().send(message, (mqs, msg, arg)->{
//            Integer queueNumber = (Integer) arg;
            Integer queueNum = Integer.parseInt(arg.toString());
            return mqs.get(queueNum);
        }, 0);



        payProducer.getProducer().send(message, (mqs, msg, arg) -> {
//            Integer queueNumber = (Integer) arg;
            Integer queueNum = Integer.parseInt(arg.toString());
            return mqs.get(queueNum);
        }, 3, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.printf("异步发送：%s  \n", sendResult);
            }

            @Override
            public void onException(Throwable e) {

            }
        });
        /*//异步发送
        payProducer.getProducer().send(message, new SendCallback(){

            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println(sendResult);
            }

            @Override
            public void onException(Throwable e) {

            }
        });*/
        logger.info("sendResult,{}", sendResult);
        return sendResult;
    }
}
