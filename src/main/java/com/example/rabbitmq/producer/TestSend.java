package com.example.rabbitmq.producer;

import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class TestSend {

    @Autowired
    RabbitTemplate rabbitTemplate;

    public void testSend() {
        //至于为什么调用这个API 后面会解释
        // 参数介绍： 交换机名字，路由建， 消息内容
        CorrelationData correlationData = new CorrelationData("订单ID");

        Map<String, Object> map = new HashMap<>();
        map.put("name", "123");
        map.put("password", "123456");

//        rabbitTemplate.convertAndSend("test_direct_exchange", "direct.key", JSON.toJSONString(map), correlationData);
        rabbitTemplate.convertAndSend("test_direct_exchange", "direct.key", map, correlationData);

        // 不存在的路由key，测试路由错误回调
//        rabbitTemplate.convertAndSend("test_direct_exchange", "direct.key1111", JSON.toJSONString(map), correlationData);
    }
}