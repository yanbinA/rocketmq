package example.batch;

import org.apache.rocketmq.common.message.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Depp
 */
public class ListSplitter implements Iterator<List<Message>>{
    private final int SIZE_LIMIT = 1024 * 1024 * 4;
    private final List<Message> messages;
    private int currIndex;
    public ListSplitter(List<Message> messages) {
        this.messages = messages;
    }

    @Override
    public boolean hasNext() {
        return currIndex < messages.size();
    }

    @Override
    public List<Message> next() {
        int leftIndex = getStartIndex();
        int nextIndex = leftIndex;
        int totalSize = 0;
        while (nextIndex < messages.size()) {
            Message message = messages.get(nextIndex);
            int tempSize = calcMessageSize(message);
            if (totalSize + tempSize <= SIZE_LIMIT) {
                totalSize += tempSize;
            } else {
                break;
            }
            nextIndex++;
        }
        currIndex = nextIndex;
        return messages.subList(leftIndex, nextIndex);
    }

    private int getStartIndex() {
        Message message = messages.get(currIndex);
        int tempSize = calcMessageSize(message);
        while (tempSize > SIZE_LIMIT) {
            // todo 可能溢出
            message = messages.get(++currIndex);
            tempSize = calcMessageSize(message);
        }
        return currIndex;
    }

    private int calcMessageSize(Message message) {
        int size = message.getBody().length + message.getTopic().length();
        Map<String, String> properties = message.getProperties();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            size += entry.getKey().length() + entry.getValue().length();
        }
        // todo 没有计算transactionId大小
        return size;
    }
}
