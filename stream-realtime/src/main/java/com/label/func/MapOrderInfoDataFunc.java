package com.label.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

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
//            result.put("payment_way",after.getString("payment_way"));
            result.put("consignee",after.getString("consignee"));
            result.put("create_time",after.getLongValue("create_time"));
            result.put("original_total_amount",after.getString("original_total_amount"));
//            result.put("order_status",after.getString("order_status"));
            result.put("total_amount",after.getString("total_amount"));
            result.put("province_id",after.getLongValue("province_id"));
//            result.put("trade_body",after.getString("trade_body"));
            long createTime = after.getLongValue("create_time");
            String pay_time_slot = determineLoginPeriod(createTime);
            result.put("pay_time_slot", pay_time_slot);
        }
        return result;
    }

    private String determineLoginPeriod(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        LocalTime time = dateTime.toLocalTime();
        int hour = time.getHour();

        if (hour < 6) return "凌晨";
        else if (hour < 9) return "早晨";
        else if (hour < 12) return "上午";
        else if (hour < 14) return "中午";
        else if (hour < 18) return "下午";
        else if (hour < 22) return "晚上";
        else return "夜间";
    }
}
