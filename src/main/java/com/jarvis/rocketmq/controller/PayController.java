package com.jarvis.rocketmq.controller;

import com.jarvis.rocketmq.jms.JMSConfig;
import com.jarvis.rocketmq.jms.PayProducer;
import com.jarvis.rocketmq.model.ProductOrder;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;

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
     * 顺序消息的作用，加入多个消息， a消费完才能消费b的情况，需要顺序消息，全局顺序的话性能差，在交易情况可能用
     * 局部顺序性能强，但是需要生产段和消费端配合使用,取唯一的东西作为选取队列id凭证
     * 顺序消息不能在异步发送和广播模式使用
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



    @RequestMapping("orderList")
    public Object orderList() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        List<ProductOrder> list= ProductOrder.getOrderList();
        for(ProductOrder order : list){
            Message message = new Message(JMSConfig.ORDER_TEST_TOPIC, "",
                    String.valueOf(order.getOrderid()), order.toString().getBytes());
            SendResult result = payProducer.getProducer().send(message, (mqs, msg, args)->{
                Long id = (Long) args;
                long index = id % mqs.size();
                return mqs.get((int) index);
            }, order.getOrderid());
            logger.info("sendOrderly:{}", result);
        }
        return new HashMap<>();
    }
}
