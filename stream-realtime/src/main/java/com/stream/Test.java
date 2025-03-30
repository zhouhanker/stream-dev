package com.stream;

import com.stream.common.utils.RedisLuaUtils;

/**
 * @Package com.stream.Test
 * @Author zhou.han
 * @Date 2024/12/29 22:45
 * @description:
 */
public class Test {
    public static void main(String[] args) {
        System.err.println(RedisLuaUtils.checkSingle("真理部"));
        System.err.println(RedisLuaUtils.checkSingle("1"));
        System.err.println(RedisLuaUtils.checkSingle("2"));
        System.err.println(RedisLuaUtils.checkSingle("3"));
    }

}
