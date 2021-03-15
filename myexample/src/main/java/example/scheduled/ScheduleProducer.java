package example.scheduled;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * 延时消息
 * RocketMQ中发送延时消息的延时时长是有限制的, 不支持任意的延时时长, 需要设置固定的固定的时长等级
 * org/apache/rocketmq/store/config/MessageStoreConfig.java
 * private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
 */
public class ScheduleProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("scheduled_message_group");
        producer.setNamesrvAddr("192.168.227.131:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("TopicTest", "tagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //设置延时级别
            message.setDelayTimeLevel(i % 3 + 1);
            producer.send(message);
        }
        producer.shutdown();
    }
}
