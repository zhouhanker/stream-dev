package com.stream;

import cn.hutool.core.date.DateUtil;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.stream.common.utils.DateTimeUtils;
import com.stream.common.utils.FileUtils;
import com.stream.common.utils.RedisUtils;
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
        System.err.println(RedisUtils.checkSingle("真理部"));
        System.err.println(RedisUtils.checkSingle("1"));
        System.err.println(RedisUtils.checkSingle("2"));
        System.err.println(RedisUtils.checkSingle("3"));
    }

}
