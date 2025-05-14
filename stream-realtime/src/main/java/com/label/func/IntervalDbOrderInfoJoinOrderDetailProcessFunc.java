package com.label.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.label.func.IntervalDbOrderInfoJoinOrderDetailProcessFunc
 * @Author zhou.han
 * @Date 2025/5/14 21:38
 * @description:
 */
public class IntervalDbOrderInfoJoinOrderDetailProcessFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject result = new JSONObject();
        result.putAll(jsonObject1);
        result.put("sku_num",jsonObject2.getLongValue("sku_num"));
        result.put("split_coupon_amount",jsonObject2.getString("sku_num"));
        result.put("sku_name",jsonObject2.getString("sku_name"));
        result.put("order_price",jsonObject2.getString("order_price"));
        result.put("detail_id",jsonObject2.getString("id"));
        result.put("order_id",jsonObject2.getString("order_id"));
        result.put("sku_id",jsonObject2.getLongValue("sku_id"));
        result.put("split_activity_amount",jsonObject2.getLongValue("split_activity_amount"));
        result.put("split_total_amount",jsonObject2.getLongValue("split_total_amount"));
        collector.collect(result);
    }
}
