package com.example.rabbitmq.util;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

@FunctionalInterface
public interface ConsumerProcessor {

    void consume(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, Channel channel, byte[] body) throws IOException, InterruptedException;
}
