package com.example.rabbitmq.util;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * 本例主要测试: 消费消息不确认的情况
 */
public class Demo01 {

    private static final ConsumerProcessor noAckProcessor = new ConsumerProcessor() {
        @Override
        public void consume(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, Channel channel, byte[] body) throws UnsupportedEncodingException {
            System.out.println(new String(body, "UTF-8"));
            //TODO 这里没有进行 ACK，那么当这个消费者连接着MQ的时候，消息处于Unacked状态，当这个消费者与MQ失去连接则会重新返回Ready状态等待重新消费
        }
    };

    private static final ConsumerProcessor ackProcessor = new ConsumerProcessor() {
        @Override
        public void consume(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, Channel channel, byte[] body) throws IOException {
            System.out.println(new String(body, "UTF-8"));
            //TODO 确认消费成功
            channel.basicAck(envelope.getDeliveryTag(), false);
        }
    };

    public static void main(String[] args) throws Exception {
        // 发送消息
        ConnectionUtil.send("测试消息", true);
        // 消费消息
//        ConnectionUtil.receive(noAckProcessor);
        ConnectionUtil.receive(ackProcessor);
    }
}
