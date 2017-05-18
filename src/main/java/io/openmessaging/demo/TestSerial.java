package io.openmessaging.demo;

import java.io.*;

/**
 * Created by YangMing on 2017/5/4.
 */
public class TestSerial {
    public static void main(String[] args) {
        Student s1 = new Student("s", "age");
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("testObject.txt"));
            oos.writeObject(s1);
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream("testObject.txt"));
            System.out.println(((Student)ois.readObject()).name);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
