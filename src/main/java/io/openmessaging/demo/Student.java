package io.openmessaging.demo;

import java.io.Serializable;

/**
 * Created by YangMing on 2017/5/4.
 */
public class Student implements Serializable{
    public String name;
    public String age;
    public Student(String name, String age) {
        this.name = name;
        this.age = age;
    }
}
