package com.label.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.label.func.IntervalJoinUserInfoLabelProcessFunc
 * @Author zhou.han
 * @Date 2025/5/12 17:25
 * @description: 用户表和用户维度补充表关联
 */
public class IntervalJoinUserInfoLabelProcessFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject result = new JSONObject();
        if (jsonObject1.getString("uid").equals(jsonObject2.getString("uid"))){
            result.putAll(jsonObject1);
            result.put("height",jsonObject2.getString("height"));
            result.put("unit_height",jsonObject2.getString("unit_height"));
            result.put("weight",jsonObject2.getString("weight"));
            result.put("unit_weight",jsonObject2.getString("unit_weight"));
        }
        collector.collect(result);
    }
}
