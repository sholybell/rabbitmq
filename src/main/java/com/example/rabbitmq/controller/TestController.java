package com.example.rabbitmq.controller;

import com.example.rabbitmq.producer.TestSend;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @Autowired
    private TestSend testSend;

    @GetMapping("/test")
    public void test(){
        testSend.testSend();
    }
}
