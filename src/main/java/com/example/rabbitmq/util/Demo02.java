package com.example.rabbitmq.util;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 本例主要测试: 预取数据与批量确认的一些情况
 */
public class Demo02 {

    private static ExecutorService ex = Executors.newFixedThreadPool(20);

    private static final ConsumerProcessor ackProcessor1 = new ConsumerProcessor() {
        @Override
        public void consume(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, Channel channel, byte[] body) throws IOException, InterruptedException {
            Thread.sleep(100);
            System.out.println("消费者1:" + new String(body, "UTF-8"));
            //TODO 确认消费成功
            channel.basicAck(envelope.getDeliveryTag(), false);
        }
    };

    private static final ConsumerProcessor ackProcessor2 = new ConsumerProcessor() {
        @Override
        public void consume(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, Channel channel, byte[] body) throws IOException, InterruptedException {
            Thread.sleep(100);
            System.out.println("消费者2:" + new String(body, "UTF-8"));
            //TODO 确认消费成功
            channel.basicAck(envelope.getDeliveryTag(), false);
        }
    };

    /**
     * 本例用于说明RabbitMQ的消费者默认情况下轮询的机制消费消息(测试前请先确认队列消息净空！！！！)
     */
    private static void prefetch1() throws Exception {

        int count = 500;
        // 生产一批消息用于消费
        for (int i = 1; i <= count; i++) {
            ConnectionUtil.send("测试消息" + i, true);
        }

        // 由于消费者启动有时间差异，当两个都启动之后，就会轮询消费
        new Thread(() -> {
            try {
                ConnectionUtil.receive(ackProcessor1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                ConnectionUtil.receive(ackProcessor2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        //TODO 通过观察，两个队列默认各自拉取200个消息去消费
    }

    // 暂时预获取数据的情况
    private static void prefetch2() throws Exception {

        int count = 500;
        // 生产一批消息用于消费
        for (int i = 1; i <= count; i++) {
            ConnectionUtil.send("测试消息" + i, true);
        }

        // 由于消费者启动有时间差异，当两个都启动之后，就会轮询消费
        new Thread(() -> {
            try {
                ConnectionUtil.receivePrefetch(ackProcessor1, 10);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                ConnectionUtil.receivePrefetch(ackProcessor2, 10);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        //TODO 强制指定每个队列每次就拉取10个
    }

    /**
     * RabbitMQ的预取数据与性能相关
     */
    private static void costTime(int count, int prefetch, boolean multiple) throws Exception {

        // 生产一批消息用于消费
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 1; i <= count; i++) {
            ex.submit(() -> {
                try {
                    ConnectionUtil.send("测试消息", false);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();

        ConnectionUtil.receivePrefetch(new ConsumerProcessor() {

            private Long startTime;
            private Long consumeCount = 0L;

            @Override
            public void consume(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, Channel channel, byte[] body) throws IOException, InterruptedException {

                if (startTime == null) {
                    startTime = System.currentTimeMillis();
                }

                consumeCount++;
                if (consumeCount == count) {
                    System.out.println("预取数据耗时:" + (System.currentTimeMillis() - startTime));
                }

                if (multiple) {
                    if (consumeCount % prefetch == 0) {
                        //TODO 当消费达到预获取消息条目时，批量确认
                        channel.basicAck(envelope.getDeliveryTag(), multiple);
                    }
                } else {
                    //TODO 非批量确认，那么每条消息都确认
                    channel.basicAck(envelope.getDeliveryTag(), multiple);
                }

            }
        }, prefetch);
    }

    /**
     * 测试当批量确认时，其中一条消息出了异常会出现啥情况
     * <p>
     * //TODO 查看RabbitMQ控制台，由于第888条信息异常，导致1-900的消息被消费（可以观察打印的数字），但是队列却留有200条消息未被处理
     * //TODO 801-900消费，但是由于888条消息抛异常，导致channel关闭，第九次确认消费失败，901-1000条消息由于前面的消息未被消费，不能拉取
     */
    private static void multipleConsumeException1(int count, int prefetch) throws Exception {

        // 生产一批消息用于消费
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 1; i <= count; i++) {
            ex.submit(() -> {
                try {
                    ConnectionUtil.send("测试消息", false);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();

        ConnectionUtil.receivePrefetch(new ConsumerProcessor() {

            private Long consumeCount = 0L;

            @Override
            public void consume(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, Channel channel, byte[] body) throws IOException, InterruptedException {

                consumeCount++;

                //TODO 强制抛异常
                if (consumeCount == 888) {
                    throw new RuntimeException("主动抛异常!!");
                }

                //TODO 查看当前消费了多少条消息
                System.out.println(consumeCount);

                if (consumeCount % prefetch == 0) {
                    //TODO 当消费达到预获取消息条目时，批量确认
                    channel.basicAck(envelope.getDeliveryTag(), true);
                }

            }
        }, prefetch);
    }


    /**
     * 测试当批量确认时，其中一条消息出了异常会出现啥情况
     * <p>
     * //TODO 查看RabbitMQ控制台，虽然888条消息异常，但是被捕获重新重新回到队列，所以除了当前消息，其他消息都能够正常消费
     * //TODO 由于这种情况，就需要注意，如果很多异常消息不断地回到队列达到一定的量，就会导致消费者一直在消费异常消息！！！！正常消息消费不及时！！！
     */
    private static void multipleConsumeException2(int count, int prefetch) throws Exception {

        // 生产一批消息用于消费
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 1; i <= count; i++) {
            ex.submit(() -> {
                try {
                    ConnectionUtil.send("测试消息", false);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();

        ConnectionUtil.receivePrefetch(new ConsumerProcessor() {

            private Long consumeCount = 0L;

            @Override
            public void consume(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, Channel channel, byte[] body) throws IOException, InterruptedException {

                consumeCount++;

                try {
                    //TODO 强制抛异常
                    if (consumeCount == 888) {
                        throw new RuntimeException("主动抛异常!!");
                    }
                } catch (Exception ex) {
                    //TODO 当前消息消费异常，执行NACK处理，并重新填充回队列
                    channel.basicNack(envelope.getDeliveryTag(), false, true);
                }

                //TODO 查看当前消费了多少条消息
                System.out.println(consumeCount);

                if (consumeCount % prefetch == 0) {
                    //TODO 当消费达到预获取消息条目时，批量确认
                    channel.basicAck(envelope.getDeliveryTag(), true);
                }

            }
        }, prefetch);
    }

    public static void main(String[] args) throws Exception {
//        prefetch1();
//        prefetch2();

        // 以下几个测试最好分开进行，否则会同时消费一个队列
        //TODO 结论: 与取数据条目越多性能越好，如果搭配批量确认，性能会更好点
//        costTime(10000, 5, false);  // 测试约1986ms  1688ms
//        costTime(10000, 500, false);  // 测试约899ms  807ms 节省时间在多次获取消息上
        //TODO 但是需要知道的时候，批量确认的时候，可能由于一批数据中一条记录异常，导致整批数据重新消费
//        costTime(10000, 500, true);  // 测试约580ms 726ms 节省时间在多次获取消息上 同时也在多次确认消费上


//        multipleConsumeException1(1000, 100);
        multipleConsumeException2(1000, 100);
    }

}
