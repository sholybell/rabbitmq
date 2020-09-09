package com.example.rabbitmq.util;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

/**
 * 本例测试：死信队列的使用
 */
public class Demo03 {

    private static final ConsumerProcessor ackProcessor = new ConsumerProcessor() {
        @Override
        public void consume(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, Channel channel, byte[] body) throws IOException {
            System.out.println(new String(body, "UTF-8"));
            //TODO 确认消费成功
            channel.basicAck(envelope.getDeliveryTag(), false);
        }
    };

    /**
     * 测试队列本身有过期时间
     */
    private static void queueWithTTL() throws Exception {
        //ttl秒钟后，消息将会被转移到死信队列
        int ttl = 15;
        for (int i = 1; i <= 10; i++) {
            ConnectionUtil.sendWithDeadQueue("test", ttl, true);
        }
        //ttl秒钟后，将会从死信队列中消费到消息(可以注释消费代码，观察死信队列消息情况，注意观察消息头存在过期时间，重试次数，进入死信队列的原因等)
        ConnectionUtil.receiveDeadQueue(ackProcessor);
    }

    /**
     * 消息本身带有过期时间，队列无过期时间
     */
    private static void messageWithTTL() throws Exception {
        //ttl秒钟后，消息将会被转移到死信队列
        int ttl = 15;
        for (int i = 1; i <= 10; i++) {
            ConnectionUtil.sendWithDeadQueueAndMsgExpiration("test", ttl * i, true);
        }
        //ttl秒钟后，将会从死信队列中消费到消息(可以注释消费代码，观察死信队列消息情况，注意观察消息头存在过期时间，重试次数，进入死信队列的原因等)
        ConnectionUtil.receiveDeadQueue(ackProcessor);
    }

    public static void main(String[] args) throws Exception {
//        queueWithTTL();
        messageWithTTL();
    }

}
