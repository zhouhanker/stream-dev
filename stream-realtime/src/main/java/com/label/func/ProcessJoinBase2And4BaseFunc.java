package com.label.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @Package com.label.func.Join2_4Process
 * @Author zhou.han
 * @Date 2025/5/15 16:06
 * @description:
 */
public class ProcessJoinBase2And4BaseFunc extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {

    private static final String[] list2 = {"18_24", "25_29", "30_34", "35_39", "40_49", "50"};

    private static final String[] list4 = {"18-24", "25-29", "30-34", "35-39", "40-49", "50"};


    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject searchWeight = new JSONObject();


        for (String s : list2) {
            double v = left.getDoubleValue("search_" + s);
            searchWeight.put(s, v);
        }

        JSONObject deviceWeight = new JSONObject();
        for (String s : list2) {
            double v = left.getDoubleValue("device_" + s);
            deviceWeight.put(s, v);
        }

        JSONObject payTimeWeight = new JSONObject();
        for (String s : list4) {
            double v = left.getDoubleValue("pay_time_" + s);
            String s1 = s.replace("-", "_");
            payTimeWeight.put(s1, v);
        }

        JSONObject b1nameWeight = new JSONObject();
        for (String s : list4) {
            double v = left.getDoubleValue("b1name_" + s);
            String s1 = s.replace("-", "_");
            b1nameWeight.put(s1, v);
        }

        JSONObject tnameWeight = new JSONObject();
        for (String s : list4) {
            double v = left.getDoubleValue("tname_" + s);
            String s1 = s.replace("-", "_");
            tnameWeight.put(s1, v);
        }

        JSONObject amountWeight = new JSONObject();
        for (String s : list4) {
            double v = left.getDoubleValue("amount_" + s);
            String s1 = s.replace("-", "_");
            amountWeight.put(s1, v);
        }


        JSONObject result = new JSONObject();
        String uid = left.getString("uid");
        String ts_ms = left.getString("ts_ms");
        result.put("uid",uid);
        result.put("ts_ms",ts_ms);

        String ageLevel = getAgeLevel(searchWeight, deviceWeight, payTimeWeight, b1nameWeight, tnameWeight, amountWeight);
        result.put("ageLevel",ageLevel);
        collector.collect(result);

    }
    public static String getAgeLevel(JSONObject search,JSONObject device,JSONObject payTime,JSONObject b1name,JSONObject tname, JSONObject amount){
        ArrayList<Double> codes = new ArrayList<>();
        for (String s : list2) {
            Double v=search.getDoubleValue(s)
                    + device.getDoubleValue(s)
                    + payTime.getDoubleValue(s)
                    + b1name.getDoubleValue(s)
                    + tname.getDoubleValue(s)
                    + amount.getDoubleValue(s);
            codes.add(v);
        }
        Double max = Collections.max(codes);
        int i = codes.indexOf(max);
        return list2[i];


    }
}
