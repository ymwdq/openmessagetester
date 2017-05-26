package io.openmessaging.demo.YmConsume;

import io.openmessaging.PullConsumer;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.YmSerial.YmChunkParser2;
import io.openmessaging.demo.YmWriteModule.YmMessageReader3;

import java.util.*;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmMessageDistributor {
    private static YmMessageDistributor distributor = new YmMessageDistributor();
    private HashMap<String, List<DefaultBytesMessage>> msgTable;
    private HashMap<String, Set<PullConsumer>> needTable;
    private HashMap<String, Set<PullConsumer>> attachTable;
    private Set<YmPullConsumer> workedConsumer;
    private Set<YmPullConsumer> waitedConsumer;
    private YmMessageReader3 ymr;
    private YmChunkParser2 chunkParser2;

    private YmMessageDistributor() {
        workedConsumer = new HashSet<>(ConsumeConfig.CONSUMER_NUM * 4);
        waitedConsumer = new HashSet<>(ConsumeConfig.CONSUMER_NUM * 4);
        ymr = YmMessageReader3.getInstance();
        chunkParser2 = new YmChunkParser2();
    }

    public static YmMessageDistributor getInstance() {
        return distributor;
    }

    public synchronized void setTable(HashMap<String, List<DefaultBytesMessage>> msgTable) {
        this.msgTable = msgTable;
    }

    public synchronized void submitConsumer(YmPullConsumer consumer, String queue, Collection<String> topics) {
        addQueueOrTopicToTable(queue, consumer, needTable);
        for (String topic : topics) {
            addQueueOrTopicToTable(topic, consumer, needTable);
        }
        waitedConsumer.add(consumer);
    }

    private void addQueueOrTopicToTable(String queue, YmPullConsumer consumer, HashMap<String, Set<PullConsumer>> table) {
        if (table.containsKey(queue)) {
            table.get(queue).add(consumer);
        } else {
            HashSet<PullConsumer> consumers = new HashSet<>();
            consumers.add(consumer);
            table.put(queue, consumers);
        }
    }

    private void initNeedTable() {
        for (String key : attachTable.keySet()) {
            for (PullConsumer consumer : attachTable.get(key)) {
                addQueueOrTopicToTable(key, (YmPullConsumer)consumer, needTable);
            }
        }
    }

    private void offer() {

    }

    private boolean isDistributedOver() {
        return true;
    }

//    public synchronized void query() {
//        if (isDistributedOver()) {
//            byte[] metaData = ymr.readDataChunk();
//            chunkParser2.setMetaData(metaData);
//            chunkParser2.readChunk();
//            msgTable = chunkParser2.getTable();
//            offer();
//        }
//    }

}
