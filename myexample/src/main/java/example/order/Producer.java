package example.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * 顺序消息, 发送的消息具有顺序性, 消费者可以按照消息的发送顺序来消费(FIFO),
 * 在默认情况下, 消息会发送到不同的queue(Round Robin轮询的方式), 消费者从不同的queue上拉取消息,这种情况无法保证消息有序.
 * 但是如果控制消息只依次发送到相同的queue, 消费者再从这个queue上消费, 就可以得到有序的消息.
 * 当发送和消费参与的queue只有一个, 则是全局有序
 * 当发送和消费参与的queue有多个, 则是分区有序, 相对每个queue消息是有序的
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //实例消息生产者
        DefaultMQProducer producer = new DefaultMQProducer("order_message_group");
        //指定ServerName
        producer.setNamesrvAddr("192.168.227.131:9876");
        //启动消息生产者
        producer.start();
        String[] tags = new String[]{"tagA", "tagB", "tagC", "tagD", "tagE"};
        for (int i = 0; i < 100; i++) {
            String body = "Hello RocketMQ + " + i;
            Message message = new Message("TopicTest", tags[i % tags.length], body.getBytes(RemotingHelper.DEFAULT_CHARSET));
            //发送消息, 自定义Queue选择器, 根据i选择Queue
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer index = (Integer) arg;
                    return mqs.get(index % mqs.size());
                }
            }, i % 10);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }
}
