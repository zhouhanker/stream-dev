package com.stream;

import cn.hutool.core.date.DateUtil;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.stream.common.utils.DateTimeUtils;
import com.stream.common.utils.FileUtils;
import com.stream.utils.SensitiveWordsUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;
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
        String s = "金沙河银丝挂面900g*3包狗屎不如！一破就漏！外卖包装都破了！吃起来像纸一样！连这点都做不好！垃圾！,gcd,发票";
        System.err.println(SensitiveWordHelper.contains(s));
        System.err.println(SensitiveWordHelper.findAll(s));
    }

    private static BigDecimal tryDecodeAndParse(String input, Charset charset) {
        try {
            byte[] bytes = input.getBytes(charset);
            return parseDecimalFromMySQLBytes(bytes, 10, 2);
        } catch (Exception e) {
            return null;
        }
    }

    private static BigDecimal parseDecimalFromMySQLBytes(byte[] bytes, int precision, int scale) {
        if (bytes == null || bytes.length == 0) {
            return BigDecimal.ZERO;
        }
        try {
            // 尝试将字节数组转换为字符串
            String str = new String(bytes, StandardCharsets.UTF_8);
            // 去除可能的非数字字符
            str = str.replaceAll("[^0-9.-]", "");
            if (str.isEmpty()) {
                return BigDecimal.ZERO;
            }
            return new BigDecimal(str).setScale(scale, BigDecimal.ROUND_HALF_UP);
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }
}
