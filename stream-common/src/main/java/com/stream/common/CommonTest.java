package com.stream.common;

import com.stream.common.utils.ConfigUtils;

/**
 * @Package com.stream.common.CommonTest
 * @Author zhou.han
 * @Date 2024/10/11 13:58
 * @description: Test
 */
public class CommonTest {

    public static void main(String[] args) {
        String kafka_err_log = ConfigUtils.getString("kafka.err.log");
        System.err.println(kafka_err_log);
    }

    public static String compareVersions(String version1, String version2) {
        String[] parts1 = version1.split("\\.");
        String[] parts2 = version2.split("\\.");
        int maxLen = Math.max(parts1.length, parts2.length);
        String[] paddedParts1 = new String[maxLen];
        String[] paddedParts2 = new String[maxLen];
        System.arraycopy(parts1, 0, paddedParts1, 0, parts1.length);
        System.arraycopy(parts2, 0, paddedParts2, 0, parts2.length);
        for (int i = parts1.length; i < maxLen; i++) {
            paddedParts1[i] = "0";
        }
        for (int i = parts2.length; i < maxLen; i++) {
            paddedParts2[i] = "0";
        }
        for (int i = 0; i < maxLen; i++) {
            int num1 = Integer.parseInt(paddedParts1[i]);
            int num2 = Integer.parseInt(paddedParts2[i]);
            if (num1 > num2) {
                return version1;
            } else if (num1 < num2) {
                return version2;
            }
        }
        return version1;
    }



}
