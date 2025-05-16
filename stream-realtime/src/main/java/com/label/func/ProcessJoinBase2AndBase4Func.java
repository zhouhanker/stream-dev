package com.label.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.label.func.Join2_4Process
 * @Author zhou.han
 * @Date 2025/5/15 16:06
 * @description:
 */
public class ProcessJoinBase2AndBase4Func extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {

    private static final String[] list2 = {"18_24", "25_29", "30_34", "35_39", "40_49", "50"};

    private static final String[] list4 = {"18-24", "25-29", "30-34", "35-39", "40-49", "50"};


    @Override
    public void processElement(JSONObject deviceValue, JSONObject orderValue, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        orderValue.putAll(deviceValue);
        orderValue.put("sum_18_24_score",
                        orderValue.getDoubleValue("b1name_18-24")+
                        orderValue.getDoubleValue("amount_18-24")+
                        orderValue.getDoubleValue("tname_18-24")+
                        orderValue.getDoubleValue("pay_time_18-24")+
                        orderValue.getDoubleValue("device_18_24")+
                        orderValue.getDoubleValue("search_18_24")
                );

        orderValue.put("sum_25-29_score",
                orderValue.getDoubleValue("b1name_25-29")+
                        orderValue.getDoubleValue("amount_25-29")+
                        orderValue.getDoubleValue("tname_25-29")+
                        orderValue.getDoubleValue("pay_time_25-29")+
                        orderValue.getDoubleValue("device_25-29")+
                        orderValue.getDoubleValue("search_25-29")
        );

        orderValue.put("sum_30-34_score",
                orderValue.getDoubleValue("b1name_30-34")+
                        orderValue.getDoubleValue("amount_30-34")+
                        orderValue.getDoubleValue("tname_30-34")+
                        orderValue.getDoubleValue("pay_time_30-34")+
                        orderValue.getDoubleValue("device_30-34")+
                        orderValue.getDoubleValue("search_30-34")
        );

        orderValue.put("sum_35_39_score",
                orderValue.getDoubleValue("b1name_35_39")+
                        orderValue.getDoubleValue("amount_35_39")+
                        orderValue.getDoubleValue("tname_35_39")+
                        orderValue.getDoubleValue("pay_time_35_39")+
                        orderValue.getDoubleValue("device_35_39")+
                        orderValue.getDoubleValue("search_35_39")
        );

        orderValue.put("sum_40_49_score",
                orderValue.getDoubleValue("b1name_40_49")+
                        orderValue.getDoubleValue("amount_40_49")+
                        orderValue.getDoubleValue("tname_40_49")+
                        orderValue.getDoubleValue("pay_time_40_49")+
                        orderValue.getDoubleValue("device_40_49")+
                        orderValue.getDoubleValue("search_40_49")
        );

        orderValue.put("sum_50_score",
                orderValue.getDoubleValue("b1name_50")+
                        orderValue.getDoubleValue("amount_50")+
                        orderValue.getDoubleValue("tname_50")+
                        orderValue.getDoubleValue("pay_time_50")+
                        orderValue.getDoubleValue("device_50")+
                        orderValue.getDoubleValue("search_50")
        );

        collector.collect(orderValue);

    }

}
