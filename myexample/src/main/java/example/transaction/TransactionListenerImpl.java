package example.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Depp
 */
public class TransactionListenerImpl implements TransactionListener {
    private AtomicInteger transactionIndex = new AtomicInteger(0);
    private ConcurrentMap<String, Integer> localTrans = new ConcurrentHashMap<>();
    /**
     *  该方法用于: half消息发送成功后, 执行本地事务, 返回事务状态
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        int index = transactionIndex.getAndIncrement();
        int status = index % 3;
        String transactionId = msg.getTransactionId();
        localTrans.put(transactionId, status);
        return LocalTransactionState.UNKNOW;
    }

    /**
     * 该方法用于 检查本地事务状态,并响应MQ的检查
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        String transactionId = msg.getTransactionId();
        int status = localTrans.get(transactionId);
        System.out.printf("%tT--%s--检查事务%s--的状态-%s%n",new Date(), Thread.currentThread().getName(), transactionId, status);
        switch (status) {
            case 0:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 1:
                return LocalTransactionState.ROLLBACK_MESSAGE;
            default:
                return LocalTransactionState.UNKNOW;
        }
    }
}
