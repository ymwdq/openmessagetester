package io.openmessaging.demo.YmWriteModule;

import io.openmessaging.MessageHeader;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.DefaultPullConsumer;
import io.openmessaging.demo.Util.YmLogUtil;
import io.openmessaging.demo.YmSerial.YmChunkParser2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmMessageReader3 {
    private int counter = 1;
    private File file;
    private RandomAccessFile raf;
    private MappedByteBuffer mbb;
    private FileChannel fileChannel;

    private final long MAX_BUFFER_SIZE = StoreConfig.MAX_BUFFER_SIZE;
    private static YmMessageReader3 ymr = new YmMessageReader3();
    private YmChunkParser2 chunkParser = new YmChunkParser2();

    public void init() throws Exception{
        file = new File(StoreConfig.STORE_PATH + StoreConfig.FILE_NAME + counter);
        if (!file.exists()) {
            throw new FileNotFoundException("file not found");
        }
        try {
            raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();
            mbb = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, MAX_BUFFER_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private YmMessageReader3() {
        chunkParser = new YmChunkParser2();
    }

    public static YmMessageReader3 getInstance() {
        return ymr;
    }

    public void readNewFile() throws Exception{
        counter++;
        init();
    }

    public byte[] readDataChunk() throws Exception{
        readNewFile();
        byte[] dataChunk = new byte[(int)MAX_BUFFER_SIZE];
        mbb.get(dataChunk);
        return dataChunk;
    }

    public void readData() throws Exception{
        init();
        byte[] dataChunk = new byte[(int)MAX_BUFFER_SIZE];
        mbb.get(dataChunk);
        chunkParser.setMetaData(dataChunk);
        chunkParser.readChunk();
        HashMap<String, List<DefaultBytesMessage>> table = chunkParser.getTable();

        List<DefaultBytesMessage> list = table.get("QUEUE_3");
        for (DefaultBytesMessage message : list) {
            String queueOrTopic;
            if (message.headers().getString(MessageHeader.QUEUE) != null) {
                queueOrTopic = message.headers().getString(MessageHeader.QUEUE);
            } else {
                queueOrTopic = message.headers().getString(MessageHeader.TOPIC);
            }
            if (queueOrTopic.equals("QUEUE_3")) {
                String body = new String(message.getBody());
                int index = body.lastIndexOf("_");
                String producer = body.substring(0, index);
                if (producer.equals("PRODUCER_1")) {
                    System.out.println(new String(message.getBody()));
                }
            }
        }
    }

    public static void main(String[] args) {
        YmLogUtil timer = new YmLogUtil();
        timer.startCount();
        StoreConfig.STORE_PATH = "d://";
        YmMessageReader3 reader = YmMessageReader3.getInstance();
        try {
            reader.readData();
        } catch (Exception e) {
            e.printStackTrace();
        }
        timer.endCount();
        timer.printTime();
    }

}
