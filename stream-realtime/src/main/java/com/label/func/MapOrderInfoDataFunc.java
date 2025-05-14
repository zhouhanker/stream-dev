package com.label.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Package com.label.func.MapOrderInfoData
 * @Author zhou.han
 * @Date 2025/5/14 14:10
 * @description:
 */
public class MapOrderInfoDataFunc extends RichMapFunction<JSONObject,JSONObject> {
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject result = new JSONObject();
        if (jsonObject.containsKey("after")){
            JSONObject after = jsonObject.getJSONObject("after");
            result.put("ts_ms",jsonObject.getLongValue("ts_ms"));
            result.put("id",after.getString("id"));
            result.put("uid",after.getString("user_id"));
            result.put("payment_way",after.getString("payment_way"));
            result.put("consignee",after.getString("consignee"));
            result.put("create_time",after.getLongValue("create_time"));
            result.put("original_total_amount",after.getString("original_total_amount"));
//            result.put("order_status",after.getString("order_status"));
            result.put("total_amount",after.getString("total_amount"));
            result.put("province_id",after.getLongValue("province_id"));
            result.put("trade_body",after.getString("trade_body"));
        }
        return result;
    }
}
