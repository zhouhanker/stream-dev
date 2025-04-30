package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.RedisLuaUtils;
import org.apache.flink.api.common.functions.RichMapFunction;


/**
 * @Package com.retailersv1.func.mapCheckRedisSensitiveWordsFuc
 * @Author zhou.han
 * @Date 2025/4/1 13:32
 * @description: check sensitive redis lua
 */
public class MapCheckRedisSensitiveWordsFunc extends RichMapFunction<JSONObject,JSONObject>{




    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject resultJson = new JSONObject();
        // 公共字段提取到外部，避免重复设置
        resultJson.put("user_id", jsonObject.getLong("user_id"));
        resultJson.put("consignee", jsonObject.getString("info_consignee"));
        resultJson.put("ts_ms",jsonObject.getLong("ts_ms"));
        resultJson.put("ds",jsonObject.getString("ds"));

        String commentTxt = jsonObject.getString("commentTxt");
        String[] words = commentTxt.split(",");
        resultJson.put("msg", commentTxt);
        String lastWord = words.length > 0 ? words[words.length - 1] : "";

        boolean isViolation = RedisLuaUtils.checkSingle(lastWord);
        resultJson.put("is_violation", isViolation ? 1 : 0);


        if (isViolation) {
            // 违规时设置违规相关字段
            resultJson.put("violation_grade", "P0");
            resultJson.put("violation_msg", lastWord);
        } else {
            // 非违规时设置默认值和额外信息
            resultJson.put("violation_grade", "");
            resultJson.put("violation_msg", "");
        }

        return resultJson;
    }
}
