package io.openmessaging.demo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by YangMing on 2017/5/4.
 */
public class YmSimpleFileUtil {
    String filePath;
    FileOutputStream writer;
    File file;
    public void createFile() {
        file =  new File(filePath);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setArgs (String path, String fileName, boolean isAppend) {
        this.filePath = path + fileName;
        file = new File(filePath);
        try {
            writer = new FileOutputStream(file, isAppend);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeContent(byte[] content) {
//        System.out.println("filepath ->" + filePath);
        try {
            writer.write(content);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void flush () {
        try {
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close () {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
