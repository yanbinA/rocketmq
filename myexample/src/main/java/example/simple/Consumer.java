package example.simple;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class Consumer {
    public static void main(String[] args) throws MQClientException {
        //实例化消费者, 指定group
        // todo DefaultMQPushConsumer和 DefaultLitePullConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("simple_message_group");
        //设置NameServer的地址
        consumer.setNamesrvAddr("192.168.227.131:9876");
        //订阅Topic, 指定tags来过滤需要消费的消息
        consumer.subscribe("TopicTest", "*");
        //注册回调实现类来处理从broker拉回的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                // todo 在消费同步消息时, 只收到一半的消息?
                // 无法重现了---当时虚拟机的网络设置了动态获取有指定了固定ip?
                System.out.printf("consumeThread=" + Thread.currentThread().getName() + "%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动消费者, 该方法必须在配置完后调用
        consumer.start();
        System.out.printf("Consumer Started.%n");
        //consumer.shutdown();
        //System.out.printf("Consumer Shutdown.%n");
    }
}
