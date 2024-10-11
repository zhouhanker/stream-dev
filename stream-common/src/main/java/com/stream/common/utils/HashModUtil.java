package com.stream.common.utils;

import cn.hutool.crypto.SecureUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author han.zhou
 * @date 2022-01-12 10:06
 **/
public class HashModUtil {

    public static String hashMod(Object input, Integer target) {
        String md5 = SecureUtil.md5(input.toString());
        String suffix3 = md5.substring(md5.length() - 3);
        Integer num = Integer.parseInt(suffix3, 16) % target;
        return String.format("%04d", num);
    }

    public static String btcHashMod(Object input) {
        return hashMod(input, 128);
    }

    public static String ethHashMod(Object input) {
        return ETH_MOD_MAP.containsKey(input.toString().toLowerCase())
                ? ETH_MOD_MAP.get(input.toString().toLowerCase())
                : hashMod(input, 128);
    }

    private static final Map<String, String> ETH_MOD_MAP = new HashMap<String, String>() {{
        put("0x0000000000000000000000000000000000000000", "9001");
        put("0xdac17f958d2ee523a2206206994597c13d831ec7", "9002");
        put("0x7a250d5630b4cf539739df2c5dacb4c659f2488d", "9003");
        put("0xea674fdde714fd979de3edf0f56aa9716b898ec8", "9004");
        put("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "9005");
        put("0x0000000000000000000000000000000000000001", "9006");
        put("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "9007");
    }};

    public static void main(String[] args) {
        System.out.println(ethHashMod("0x0000000000000000000000000000000000000000"));
    }

}
