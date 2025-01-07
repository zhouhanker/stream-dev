package com.stream;

import com.stream.common.utils.FileUtils;

/**
 * @Package com.stream.Test
 * @Author zhou.han
 * @Date 2024/12/29 22:45
 * @description:
 */
public class Test {
    public static void main(String[] args) {
        String fullClassName = "com.retailersv1.DbusLogDataProcess2Kafka";
        System.err.println(getClassName(fullClassName));
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
