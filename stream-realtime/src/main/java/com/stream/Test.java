package com.stream;

import com.stream.common.utils.FileUtils;

import java.util.Locale;
import java.util.Objects;

/**
 * @Package com.stream.Test
 * @Author zhou.han
 * @Date 2024/12/29 22:45
 * @description:
 */
public class Test {
    public static void main(String[] args) {
        String FLINK_REMOTE_JAR_PATH = "hdfs://cdh01:8020/flink-jars/";
        String fullClassName = "com.retailersv1.DbusLogDataProcess2Kafka";
        String path =  FLINK_REMOTE_JAR_PATH + getClassName(fullClassName) + ".jar";
        System.err.println(path);
    }

    public static String getClassName(String fullClassName) {
        String[] parts = fullClassName.split("\\.");
        if (parts.length > 0) {
            String lastName = parts[parts.length - 1];
            return lastName.trim();
        }
        return null;
    }
}
