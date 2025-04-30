package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.retailersv1.func.IntervalJoinOrderCommentAndOrderInfoFunc
 * @Author zhou.han
 * @Date 2025/3/16 16:28
 * @description: orderComment Join orderInfo Msg
 */
public class IntervalJoinOrderCommentAndOrderInfoFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject comment, JSONObject info, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out){
        JSONObject enrichedComment = (JSONObject)comment.clone();

        for (String key : info.keySet()) {
            enrichedComment.put("info_" + key, info.get(key));
        }
        out.collect(enrichedComment);
    }
}
