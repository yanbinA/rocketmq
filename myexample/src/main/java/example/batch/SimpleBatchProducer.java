package example.batch;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * 批量消息, 批量发送消息能显著提高发送小消息的性能.
 * 限制: 相同的Topic, 相同的waitStoreMsgOK, 不能是延时消息, 而且这批消息的总大小不能超过4MB
 * todo waitStoreMsgOK什么作用
 */
public class SimpleBatchProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //实例消息生产者
        DefaultMQProducer producer = new DefaultMQProducer("batch_message_group");
        //指定ServerName
        producer.setNamesrvAddr("192.168.227.131:9876");
        producer.start();
        List<Message> list = new ArrayList<>();
        list.add(new Message("TopicTest", "*", "Hello RocketMQ 1".getBytes(RemotingHelper.DEFAULT_CHARSET)));
        list.add(new Message("TopicTest", "*", "Hello RocketMQ 2".getBytes(RemotingHelper.DEFAULT_CHARSET)));
        list.add(new Message("TopicTest", "*", "Hello RocketMQ 3".getBytes(RemotingHelper.DEFAULT_CHARSET)));
        producer.send(list);
        producer.shutdown();
    }
}
