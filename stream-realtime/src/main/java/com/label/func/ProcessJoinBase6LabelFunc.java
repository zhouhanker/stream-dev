package com.label.func;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.DateTimeUtils;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @Package com.label.func.ProcessJoinBase6LabelFunc
 * @Author zhou.han
 * @Date 2025/5/16 14:04
 * @description:
 */
public class ProcessJoinBase6LabelFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject value1, JSONObject value2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        value1.putAll(value2);
        JSONObject result = new JSONObject();
        JSONObject resultPage = new JSONObject();
        JSONObject resultDevice = new JSONObject();
        JSONObject resultCalcu = new JSONObject();
        JSONObject resultScore = new JSONObject();
        JSONObject resultBaseLine = new JSONObject();
        JSONObject supResult = new JSONObject();
        double sum30_40 = value1.getDoubleValue("sum_30-34_score");
        double sum50 = value1.getDoubleValue("sum_50_score");
        double sum25_29 = value1.getDoubleValue("sum_25-29_score");
        double sum40_49 = value1.getDoubleValue("sum_40_49_score");
        double sum18_24 = value1.getDoubleValue("sum_18_24_score");
        double sum35_39 = value1.getDoubleValue("sum_35_39_score");
        Map<String, Double> maxScore = getMaxScore(value1);
        result.put("uid",value1.getString("uid"));
        result.put("uname",value1.getString("consignee"));
        result.put("ts_ms",value1.getLongValue("ts_ms"));

        resultBaseLine.put("decade",value1.getLongValue("decade"));
        resultBaseLine.put("zodiac_sign",value1.getString("zodiac_sign"));
        resultBaseLine.put("gender",value1.getString("gender"));
        resultBaseLine.put("login_name",value1.getString("login_name"));
        resultBaseLine.put("email",value1.getString("email"));
        resultBaseLine.put("phone_num",value1.getString("phone_num"));
        resultBaseLine.put("height",value1.getString("height"));
        resultBaseLine.put("judge_os",value1.getString("judge_os"));
        resultBaseLine.put("weight",value1.getString("weight"));
        resultBaseLine.put("user_level",value1.getString("user_level"));
        resultBaseLine.put("birthday",value1.getString("birthday"));
        resultBaseLine.put("base_age",value1.getLongValue("age"));
        result.put("user_base_label",resultBaseLine);

        resultPage.put("intraday_pv",value1.getLongValue("pv"));
        resultPage.put("search_item",value1.getString("search_item"));
        result.put("user_log_label",resultPage);

        resultDevice.put("md",value1.getString("md"));
        resultDevice.put("ba",value1.getString("ba"));
        resultDevice.put("os",value1.getString("os"));
        resultDevice.put("ch",value1.getString("ch"));
        resultDevice.put("login_time_slot",value1.getString("pay_time_slot"));
        result.put("user_device_label",resultDevice);

        resultScore.put("age_sum30_40",sum30_40);
        resultScore.put("age_sum50",sum50);
        resultScore.put("age_sum25_29",sum25_29);
        resultScore.put("age_sum40_49",sum40_49);
        resultScore.put("age_sum18_24",sum18_24);
        resultScore.put("age_sum35_39",sum35_39);

        resultCalcu.put("computer_age_info",resultScore);
        resultCalcu.put("spending_pow",value1.getString("spending_pow"));
        resultCalcu.put("calculate_age",maxScore);

        supResult.put("sku_name",value1.getString("sku_name"));
        supResult.put("b1_name",value1.getString("b1_name"));
        supResult.put("tname",value1.getString("tname"));
        supResult.put("sku_id",value1.getString("sku_id"));
        result.put("sup_info_message",supResult);
        result.put("user_calculate_label",resultCalcu);
        result.put("ds", DateTimeUtils.format(new Date(value1.getLong("ts_ms")), "yyyyMMdd"));


        collector.collect(result);

    }

    public static Map<String, Double> getMaxScore(JSONObject jsonObject){
        Map<String, Double> result = new HashMap<>();
        double maxValue = Double.MIN_VALUE;
        String maxKey = null;

        for (String key : jsonObject.keySet()) {
            if (key.startsWith("sum")) {
                Object value = jsonObject.get(key);
                if (value instanceof Number) {
                    double currentValue = ((Number) value).doubleValue();
                    if (currentValue > maxValue) {
                        maxValue = currentValue;
                        maxKey = key;
                    }
                }
            }
        }

        if (maxKey != null) {
            result.put(maxKey, maxValue);
        }
        return result;
    }

}
