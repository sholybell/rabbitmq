package com.example.rabbitmq.util;

import com.rabbitmq.client.*;
import org.springframework.amqp.core.ExchangeTypes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ConnectionUtil {

    private static final String QUEUE_NAME = "test_queue";
    private static final String EXCHANGE_NAME = "test_exchange";

    private static final String DEAD_QUEUE_NAME = "test_dead_queue";
    private static final String DEAD_EXCHANGE_NAME = "test_dead_exchange";


    private static Connection connection;

    static {
        try {
            connection = getConnection();
            // 测试使用删除上面建立的两个队列和两个交换机
            Channel channel = connection.createChannel();
            channel.queueDelete(QUEUE_NAME);
            channel.queueDelete(DEAD_QUEUE_NAME);
            channel.exchangeDelete(EXCHANGE_NAME);
            channel.exchangeDelete(DEAD_EXCHANGE_NAME);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Connection getConnection() throws Exception {
        //创建一个连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        //设置rabbitmq 服务端所在地址 我这里在本地就是本地
        connectionFactory.setHost("localhost");
        //设置端口号，连接用户名，虚拟地址等
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("lansun");
        connectionFactory.setPassword("123456");
        connectionFactory.setVirtualHost("bluestore");
        return connectionFactory.newConnection();
    }


    /**
     * 生产消息
     *
     * @param msg 消息内容
     */
    public static void send(String msg, boolean printLog) throws Exception {
        Connection connection = getConnection();
        Channel channel = connection.createChannel();

        //声明队列
        channel.queueDeclare(ConnectionUtil.QUEUE_NAME, false, false, true, null);
        // 声明exchange
        channel.exchangeDeclare(ConnectionUtil.EXCHANGE_NAME, ExchangeTypes.FANOUT, false, true, null);
        //交换机和队列绑定
        channel.queueBind(ConnectionUtil.QUEUE_NAME, ConnectionUtil.EXCHANGE_NAME, "");
        channel.basicPublish(ConnectionUtil.EXCHANGE_NAME, "", null, msg.getBytes());
        if (printLog) {
            System.out.println("发送的信息为:" + msg);
        }
        channel.close();
        connection.close();
    }

    /**
     * 生产消息
     *
     * @param msg 消息内容
     */
    public static void sendWithDeadQueue(String msg, int ttl, boolean printLog) throws Exception {
        Connection connection = getConnection();
        Channel channel = connection.createChannel();

        //声明死信队列
        channel.queueDeclare(ConnectionUtil.DEAD_QUEUE_NAME, false, false, true, null);
        //声明死信交换器
        channel.exchangeDeclare(ConnectionUtil.DEAD_EXCHANGE_NAME, ExchangeTypes.FANOUT, false, true, null);
//        //绑定死信队列和死信交换器
        channel.queueBind(ConnectionUtil.DEAD_QUEUE_NAME, ConnectionUtil.DEAD_EXCHANGE_NAME, "");

        //声明队列
        Map<String, Object> queueArg = new HashMap<>();
        queueArg.put("x-message-ttl", ttl * 1000);        // 单位毫秒(这里指定了队列内所有消息的统一过期时间)
        queueArg.put("x-dead-letter-exchange", DEAD_EXCHANGE_NAME);   // 死信交换器
        queueArg.put("x-dead-letter-routing-key", "");   // 路由键，直接使用fanout类型的死信交换器，不设置路由键

        channel.queueDeclare(ConnectionUtil.QUEUE_NAME, false, false, true, queueArg);
        // 声明exchange
        channel.exchangeDeclare(ConnectionUtil.EXCHANGE_NAME, ExchangeTypes.FANOUT, false, true, null);
        //交换机和队列绑定
        channel.queueBind(ConnectionUtil.QUEUE_NAME, ConnectionUtil.EXCHANGE_NAME, "");
        channel.basicPublish(ConnectionUtil.EXCHANGE_NAME, "", null, msg.getBytes());

        if (printLog) {
            System.out.println("发送的信息为:" + msg);
        }
        channel.close();
        connection.close();
    }

    /**
     * 生产消息
     *
     * @param msg 消息内容
     */
    public static void sendWithDeadQueueAndMsgExpiration(String msg, int ttl, boolean printLog) throws Exception {
        Connection connection = getConnection();
        Channel channel = connection.createChannel();

        //声明死信队列
        channel.queueDeclare(ConnectionUtil.DEAD_QUEUE_NAME, false, false, true, null);
        //声明死信交换器
        channel.exchangeDeclare(ConnectionUtil.DEAD_EXCHANGE_NAME, ExchangeTypes.FANOUT, false, true, null);
//        //绑定死信队列和死信交换器
        channel.queueBind(ConnectionUtil.DEAD_QUEUE_NAME, ConnectionUtil.DEAD_EXCHANGE_NAME, "");

        //声明队列
        Map<String, Object> queueArg = new HashMap<>();
        queueArg.put("x-dead-letter-exchange", DEAD_EXCHANGE_NAME);   // 死信交换器
        queueArg.put("x-dead-letter-routing-key", "");   // 路由键，直接使用fanout类型的死信交换器，不设置路由键

        channel.queueDeclare(ConnectionUtil.QUEUE_NAME, false, false, true, queueArg);
        // 声明exchange
        channel.exchangeDeclare(ConnectionUtil.EXCHANGE_NAME, ExchangeTypes.FANOUT, false, true, null);
        //交换机和队列绑定
        channel.queueBind(ConnectionUtil.QUEUE_NAME, ConnectionUtil.EXCHANGE_NAME, "");

        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        builder.expiration(String.valueOf(ttl * 1000));// 设置TTL
        AMQP.BasicProperties properties = builder.build();
        channel.basicPublish(ConnectionUtil.EXCHANGE_NAME, "", properties, msg.getBytes());

        if (printLog) {
            System.out.println("发送的信息为:" + msg);
        }
        channel.close();
        connection.close();
    }

    /**
     * 消费消息
     *
     * @param processor 消息消费逻辑
     */
    public static void receive(ConsumerProcessor processor) throws Exception {
        Connection connection = ConnectionUtil.getConnection();
        Channel channel = connection.createChannel();
        DefaultConsumer deliverCallback = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    processor.consume(consumerTag, envelope, properties, channel, body);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        channel.basicConsume(ConnectionUtil.QUEUE_NAME, deliverCallback);
        //TODO 消费者这里不要关闭连接，由于消费是异步进行的一旦关闭连接，可能造成消息消费情况不确定
//        channel.close();
//        connection.close();
    }

    /**
     * 消费消息
     *
     * @param processor 消息消费逻辑
     */
    public static void receivePrefetch(ConsumerProcessor processor, Integer prefetchCount) throws Exception {
        Connection connection = ConnectionUtil.getConnection();
        Channel channel = connection.createChannel();
        DefaultConsumer deliverCallback = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    processor.consume(consumerTag, envelope, properties, channel, body);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        // 设置一次拉取多少条消息
        channel.basicQos(prefetchCount);

        // TODO 本条语句开始消费，设置参数需要放在本语句之上
        channel.basicConsume(ConnectionUtil.QUEUE_NAME, deliverCallback);

        //TODO 消费者这里不要关闭连接，由于消费是异步进行的一旦关闭连接，可能造成消息消费情况不确定
//        channel.close();
//        connection.close();
    }

    /**
     * 消费消息
     *
     * @param processor 消息消费逻辑
     */
    public static void receiveDeadQueue(ConsumerProcessor processor) throws Exception {
        Connection connection = ConnectionUtil.getConnection();
        Channel channel = connection.createChannel();
        DefaultConsumer deliverCallback = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    processor.consume(consumerTag, envelope, properties, channel, body);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        channel.basicConsume(ConnectionUtil.DEAD_QUEUE_NAME, deliverCallback);
        //TODO 消费者这里不要关闭连接，由于消费是异步进行的一旦关闭连接，可能造成消息消费情况不确定
//        channel.close();
//        connection.close();
    }

}