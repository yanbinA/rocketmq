package example.simple;

import example.LocalProperty;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

/**
 * 发送异步消息, 通过接受回调来确认发送是否成功, 比较可靠
 * 多应用于: 对响应时间比较敏感的场景
 */
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //实例消息生产者
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setProducerGroup("simple_message_group");
        //设置NameServer地址
        producer.setNamesrvAddr(LocalProperty.SERVER_NAME);
        //启动Producer实例
        producer.start();
        //设置异步发送重试次数
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.setInstanceName("SIMPLE_MESSAGE");
        int count = 10;
        // 这玩意有啥用? 线程计数器
        final CountDownLatch2 countDownLatch = new CountDownLatch2(count);
        for (int i = 0; i < count; i++) {
            final int index = i;
            //创建消息, 指定Topic, tags和消息体
            // todo 弄清楚Message各属性的作用
            Message message = new Message("TopicTestA", "TagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 发送消息, SendCallback接收异步返回结果的回调
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }
        // 等待5s
        countDownLatch.await(5, TimeUnit.SECONDS);
        //关闭实例
        //producer.shutdown();

    }
}
