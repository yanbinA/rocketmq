package example.simple;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * 单向发送消息, 不会返回发送结果
 * 一般用于不重要的场景
 */
public class OnewayProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException {
        //实例消息生产者
        DefaultMQProducer producer = new DefaultMQProducer("simple_message_group");
        producer.setNamesrvAddr("192.168.227.131:9876");
        //启动生产者
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("TopicTest", "TagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.sendOneway(message);
        }
        //关闭Producer实例
        producer.shutdown();
    }
}
