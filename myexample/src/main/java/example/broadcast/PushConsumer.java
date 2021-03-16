package example.broadcast;

import example.LocalProperty;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class PushConsumer {
    public static void main(String[] args) throws MQClientException {
        //实例化消费者, 指定group
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("simple_message_group");
        //设置NameServer的地址
        consumer.setNamesrvAddr(LocalProperty.SERVER_NAME);
        //订阅Topic, 指定tags来过滤需要消费的消息
        consumer.subscribe("TopicTest", "*");
        consumer.setMessageModel(MessageModel.BROADCASTING);
        //注册回调实现类来处理从broker拉回的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("consumeThread=" + Thread.currentThread().getName() + "%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动消费者, 该方法必须在配置完后调用
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
