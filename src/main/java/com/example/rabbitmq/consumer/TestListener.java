package com.example.rabbitmq.consumer;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class TestListener {

    @RabbitListener(queues = "test_queue")
    public void get(String message) throws Exception {
        System.out.println("消费者1" + message);
    }

    @RabbitListener(queues = "test_queue")
    public void get(Message message) throws Exception {
        System.out.println("消费者2" + new String(message.getBody(), "UTF-8"));
    }
}