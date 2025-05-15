package com.label.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.label.func.Interval4BaseJoin2BaseFunc
 * @Author zhou.han
 * @Date 2025/5/15 14:44
 * @description:
 */
public class Interval4BaseJoin2BaseFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        System.err.println("jsonObject1 -> "+ jsonObject1);
        System.err.println("jsonObject2 -> "+ jsonObject2);
    }
}
