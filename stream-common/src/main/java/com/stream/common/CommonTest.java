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
        System.err.println(ConfigUtils.getString("mysql.host"));
    }

}
