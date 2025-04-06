package com.stream;

import com.github.houbb.sensitive.word.core.SensitiveWord;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.stream.common.utils.RedisLuaUtils;
import org.apache.derby.catalog.UUID;

import java.util.List;

/**
 * @Package com.stream.Test
 * @Author zhou.han
 * @Date 2024/12/29 22:45
 * @description:
 */
public class Test {
    public static void main(String[] args) {
//        String s = "画像屹立在天安门前，教员,垃圾，cnm,五星红旗";
//        List<String> all = SensitiveWordHelper.findAll(s);
//        System.err.println(String.join(", ",all));

        System.err.println(UUID.UUID_BYTE_LENGTH);

    }

}
