package example.transaction;

import example.LocalProperty;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 消息事务
 * 采用了2PC的思想来实现提交消息事务, 同时提供补偿逻辑来处理二阶段超时或失败的消息.确保了发送消息和本地事务的原子性
 * 消息事务的局限性:
 * 1. 不支持延时消息和批量消息
 * 2. 补偿逻辑的检查次数有限制(默认transactionCheckMax=15), 当检查次数超过指定次数后broker会丢弃该消息(AbstractTransactionCheckListener)
 * 3. 补偿阶段用于解决消息Commit或Rollback发生超时或者失败的情况. 超时时间在broker配置"transactionTimeout"参数, 也可以在消息中
 * 加入property.CHECK_IMMUNITY_TIME_IN_SECONDS, 该参数优先于"transactionTimeout"
 * 4. 事务消息可能被检查或消费多次
 * 5. 二阶段提交的消息重投到生产者指定的topic时,可能会失败, 这就依赖于日志High availability is ensured by the high availability mechanism of RocketMQ itself.
 * 如果要确保消息不会丢失和事务的完整性, 建议使用同步双写机制
 * 6. Producer IDs of transactional messages cannot be shared with producer IDs of other types of messages.
 * Unlike other types of message, transactional messages allow backward queries. MQ Server query clients by their Producer IDs.
 *
 * 事务的状态:
 * TransactionStatus.CommitTransaction: 提交事务, 允许消费者消费此消息
 * TransactionStatus.RollbackTransaction: 回滚事务, 这意味这该消息会被删除, 不允许消费
 * TransactionStatus.Unknown: 未知状态,意味着需要检查消息队列来确认状态
 *
 * 事务消息的流程
 * 正常事务提交:
 * 1. 提交消息(half消息, 消费者不可见)
 * 2. 服务器响应提交结果
 * 3. 执行本地事务(2步结果失败 不会执行)
 * 4. 根据本地事务的状态执行Commit或rollback(Commit操作后, 消息对消费者可见)
 * 补偿阶段:
 * 1. 对没有Commit/Rollback的事务消息, 发起一次回查
 * 2. 生产者根据本地事务状态响应回查
 * 3. 根据回查的结果执行Commit或rollback
 */
public class TransactionProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException {
        //使用TransactionMQProducer类创建生产者实例, 指定唯一producerGroup
        TransactionMQProducer producer = new TransactionMQProducer("transaction_message_group");
        producer.setNamesrvAddr(LocalProperty.SERVER_NAME);
        //设置自定义线程池来处理检查请求
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(2, 5, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(200), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("producer-transaction-check-thread");
                return thread;
            }
        });
        //执行本地事务后、需要根据执行结果对消息队列响应事务状态
        TransactionListenerImpl transactionListener = new TransactionListenerImpl();
        producer.setExecutorService(poolExecutor);
        producer.setTransactionListener(transactionListener);
        //启动实例
        producer.start();
        String[] tags = new String[]{"tagA", "tagB", "tagC"};
        for (int i = 0; i < 10; i++) {
            try {
                Message message = new Message("TopicTest", tags[i % tags.length], "KEY" + i, ("Hello RocketMQ T " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                message.putUserProperty("CHECK_IMMUNITY_TIME_IN_SECONDS", "1");
                // todo arg 参数的作用
                TransactionSendResult sendResult = producer.sendMessageInTransaction(message, null);
                System.out.printf("%s%n", sendResult);
                Thread.sleep(5);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //producer.shutdown();
    }

}
