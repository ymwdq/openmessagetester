package io.openmessaging.demo.YmConsume;

import io.openmessaging.PullConsumer;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.YmSerial.YmChunkParser2;
import io.openmessaging.demo.YmWriteModule.YmMessageReader3;

import java.io.FileNotFoundException;
import java.util.*;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmMessageDistributor {
    private static YmMessageDistributor distributor = new YmMessageDistributor();
    private HashMap<String, List<DefaultBytesMessage>> msgTable;
    private HashMap<String, Set<YmPullConsumer>> needTable;
    private HashMap<String, Set<YmPullConsumer>> attachTable;
    private Set<YmPullConsumer> waitedConsumer;
    private Set<String> unusedMsgListNames;
    private YmMessageReader3 ymr;
    private YmChunkParser2 chunkParser2;
    private int consumeOverNum;
    private boolean hasInit;

    private YmMessageDistributor() {
        waitedConsumer = new HashSet<>(ConsumeConfig.CONSUMER_NUM * 4);
        attachTable = new HashMap<>(ConsumeConfig.ATTACH_TABLE_SIZE);
        needTable = new HashMap<>(ConsumeConfig.ATTACH_TABLE_SIZE);
        unusedMsgListNames = new HashSet<>(ConsumeConfig.NAMES_TABLE_SIZE);
        ymr = YmMessageReader3.getInstance();
        chunkParser2 = new YmChunkParser2();
        new ReadChunkThread().start();
        hasInit = false;
    }

    private void init() {
        unusedMsgListNames = new HashSet<>(ConsumeConfig.NAMES_TABLE_SIZE);
        for (String key : attachTable.keySet()) {
            unusedMsgListNames.add(key);
        }
        consumeOverNum = genConsumeNum();
        hasInit = true;
    }

    public static YmMessageDistributor getInstance() {
        return distributor;
    }

    public synchronized void setTable(HashMap<String, List<DefaultBytesMessage>> msgTable) {
        this.msgTable = msgTable;
    }

    public synchronized void submitConsumer(YmPullConsumer consumer, String queue, Collection<String> topics) {
        addQueueOrTopicToTable(queue, consumer, needTable);
        addQueueOrTopicToTable(queue, consumer, attachTable);
        for (String topic : topics) {
            addQueueOrTopicToTable(topic, consumer, needTable);
            addQueueOrTopicToTable(topic, consumer, attachTable);
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        waitedConsumer.add(consumer);
    }

    private void addQueueOrTopicToTable(String queue, YmPullConsumer consumer, HashMap<String, Set<YmPullConsumer>> table) {
        if (table.containsKey(queue)) {
            table.get(queue).add(consumer);
        } else {
            HashSet<YmPullConsumer> consumers = new HashSet<>();
            consumers.add(consumer);
            table.put(queue, consumers);
        }
    }

    private void offer() {

        for (String unusedMsgListName : unusedMsgListNames) {
            Set<YmPullConsumer> needConsumers = needTable.get(unusedMsgListName);
            for (YmPullConsumer consumer : needConsumers) {
                if (waitedConsumer.contains(consumer)) {
                    consumer.setMsgList(msgTable.get(unusedMsgListName), unusedMsgListName);
                    needTable.get(unusedMsgListName).remove(consumer);
                    waitedConsumer.remove(consumer);
                    System.out.println("current consumer" + consumer);
                    System.out.println("consumer list " + consumer.getMsgList().size());
                    return;
                }
            }
        }
    }

    private boolean isDistributedOver() {
        if (consumeOverNum == 0) System.out.println("== distribute consume over : consumeOverNum == 0");
        else if (msgTable == null) System.out.println("== distribute consume over : msgTable is null");
//        else System.out.println("distribute not over");
        return consumeOverNum == 0 || msgTable == null;
    }

    private int genConsumeNum() {
        int cnt = 0;
        for (String key : msgTable.keySet()) {
            cnt += attachTable.get(key).size();
        }
        return cnt;
    }

    public synchronized void consumeOver(YmPullConsumer consumer) {
        waitedConsumer.add(consumer);
        unusedMsgListNames.add(consumer.getCurrentTopicOrQueue());
        consumeOverNum--;
        notifyAll();
    }

    public synchronized void query(YmPullConsumer consumer) {
        System.out.println(Thread.currentThread().getName() + "query");
        if (!isDistributedOver()) {
            offer();
            if (consumer.isEmpty()) {
                waitedConsumer.add(consumer);
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else {
            waitedConsumer.add(consumer);
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void readDataChunk() {
        System.out.println("distribute over read new file =====");
        byte[] metaData = null;
        try {
            metaData = ymr.readDataChunk();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("read over");
        } catch (Exception e) {
            e.printStackTrace();
        }
        chunkParser2.setMetaData(metaData);
        chunkParser2.readChunk();
        System.out.println("read chunk over");
        msgTable = chunkParser2.getTable();
        System.out.println("set table over");

    }

    class ReadChunkThread extends Thread {
        @Override
        public void run() {
            while (true) {
                if (isDistributedOver()) {
                    System.out.println("distribute over read new file =====");
                    byte[] metaData = null;
                    try {
                        metaData = ymr.readDataChunk();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                        System.out.println("read over");
                        break;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    chunkParser2.setMetaData(metaData);
                    chunkParser2.readChunk();
                    System.out.println("read chunk over");
                    msgTable = chunkParser2.getTable();
                    System.out.println("set table over");
                    distributor.init();
                    synchronized (distributor) {
                        distributor.notifyAll();
                    }
                }
            }
        }
    }
}
