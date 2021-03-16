package example.simple;

import com.sun.xml.internal.bind.v2.TODO;
import example.LocalProperty;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * 发送同步消息, 会有发送是否成功的应答, 比较可靠
 * 多应用于: 重要的消息通知, 短信通知
 */
public class SyncProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //实例消息生产者
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setProducerGroup("simple_message_group");
        //设置NameServer地址
        producer.setNamesrvAddr(LocalProperty.SERVER_NAME);
        //启动Producer实例
        producer.start();
        for (int i = 0; i < 10; i++) {
            //创建消息, 指定Topic, tags和消息体
            // todo 弄清楚Message各属性的作用
            Message message = new Message("TopicTest", "TagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 发送消息, SendResult可用来确认消息是否发送成功
            SendResult send = producer.send(message);
            System.out.printf("%s%n", send);
        }
        //关闭实例
        producer.shutdown();
    }
}
