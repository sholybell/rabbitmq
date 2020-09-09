package com.example.rabbitmq.config;

import com.alibaba.fastjson.JSON;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitmqConfig {

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost", 5672); //这里直接在构造方法传入了
        // connectionFactory.setHost();
        // connectionFactory.setPort();
        connectionFactory.setUsername("lansun");
        connectionFactory.setPassword("123456");
        connectionFactory.setVirtualHost("bluestore");
        // 是否开启发送方消息确认机制
        connectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        return connectionFactory;
    }

    @Bean
    public DirectExchange defaultExchange() {
        Map<String, Object> map = new HashMap<>();
        // 指定备选交换机
        map.put("alternate-exchange", "weixin.test_connect");
        return new DirectExchange("test_direct_exchange", false, false, map);
    }

    @Bean
    public Queue queue() {
        //名字 是否持久化
        return new Queue("test_queue", true);
    }

    @Bean
    public Binding binding() {
        //绑定一个队列 to: 绑定到哪个交换机上面 with：绑定的路由建（routingKey）
        return BindingBuilder.bind(queue()).to(defaultExchange()).with("direct.key");
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        //注意 这个ConnectionFactory 是使用javaconfig方式配置连接的时候才需要传入的 如果是yml配置 的连接的话是不需要的
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);

        // 这里是生产者到交换机过程失败，确认提交结果
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            System.out.println("确认提交回调-------------->");
            System.out.println(correlationData);
            // 判断是否有发送成功
            System.out.println("ack:" + ack);
            System.out.println("cause:" + cause);
        });

        // 开始消息从交换器到队列过程失败回调功能
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> {
            System.out.println("路由异常回调-------------->");
            // message : 相当于发送消息内容 + 发送消息的配置
            System.out.println("message:" + message);
            // replyCode : 错误状态码
            System.out.println("replyCode:" + replyCode);
            // replyText : 错误状态提示文本
            System.out.println("replyText:" + replyText);
            System.out.println("exchange:" + exchange);
            System.out.println("routingKey:" + routingKey);
        });

        rabbitTemplate.setMessageConverter(new MessageConverter() {
            // 发送消息的时候转换
            @Override
            public Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
                System.out.println("调用了消息解析器(发送前)...");
                return new Message(JSON.toJSONBytes(object), messageProperties);
            }

            // 接受消息的时候转换
            @Override
            public Object fromMessage(Message message) throws MessageConversionException {
                return null;
            }
        });

        return rabbitTemplate;
    }

}
