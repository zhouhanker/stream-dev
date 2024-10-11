package com.stream.common.utils;

import cn.hutool.core.util.HexUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * @author han.zhou
 * @time: 2021/10/15 9:47
 * @className: CommonUtils
 * @description CommonUtils
 */
public class CommonUtils {
    // 16进制 转 10进制
    public static String binaryConvert(String input, Boolean is_need_divide) {
        if (input != null) {
            BigDecimal result;
            if (input.startsWith("0x")) {
                result = new BigDecimal(HexUtil.toBigInteger(input.substring(2)));
            } else {
                result = new BigDecimal(input);
            }
            if (is_need_divide) {
                result = result.divide(BigDecimal.valueOf(1000000000000000000L));//转换单位，1Ether=10^18wei
            }
            return result.toPlainString();
        } else {
            return "0";
        }
    }

    // 向 hash list 追加新元素
    public static String operateHashList(String hashListStr, String newElement) {
        List<String> hashList;
        if (hashListStr != null) {
            hashList = JSON.parseObject(hashListStr, new TypeReference<List<String>>() {
            });
        } else {
            hashList = new ArrayList<>();
        }
        hashList.add(newElement);
        return hashList.toString();
    }

    public static List<Integer> generateDayPartitionRange(LocalDate from, LocalDate to) {
        List<Integer> res = new ArrayList<>();

        while (from.compareTo(to) < 1) {
            res.add(from.getYear() * 10000 + from.getMonthValue() * 100 + from.getDayOfMonth());
            from = from.plusDays(1);
        }

        return res;
    }

    /**
     * idea 环境 判断是否是本地 IDEA的调试或者运行模式
     */
    public static boolean isIdeaEnv() {
        List<String> arguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        for (String str : arguments) {
            if (str.toLowerCase().contains("intellij")) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取异常的详细信息
     */
    public static String getStackTraceInfo(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        try {
            e.printStackTrace(pw);
            pw.flush();
            sw.flush();
            return sw.toString();
        } finally {
            try {
                pw.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                sw.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static List<String> data2List(String data) {
        if (StringUtils.isEmpty(data)) {
            return new ArrayList<>();
        }

        List<String> res = new ArrayList<>(data.length() / 64);
        for (int i = 0; i < data.length() / 64; i++) {
            res.add("0x" + data.substring(2 + 64 * i, 2 + 64 * (i + 1)));
        }
        return res;
    }

    public static void main(String[] args) {


        for (Object item : data2List("0x000000000000000000000000000000000000000065e85842466dbd8bab15370900000000000000000000000000000000000000000000000144eff30ef070332e000000000000000000000000000000000000000065e85842466dbd8bab15370900000000000000000000000000000000000000000000000144eff30ef070332e000000000000000000000000000000000000000065e85842466dbd8bab15370900000000000000000000000000000000000000000000000144eff30ef070332e")) {
            System.out.println(item);
        }

//        System.out.print("123");

    }

    public static boolean isStage() {
        return "stage".equals(ConfigUtils.getString("env"));
    }

    public static boolean isProd() {
        return "prod".equals(ConfigUtils.getString("env"));
    }
}
