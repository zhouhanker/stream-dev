package com.trafficV1.func;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.sql.BatchUpdateException;
import java.text.ParseException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * @Package com.trafficV1.func.MapSuppTsFunc
 * @Author zhou.han
 * @Date 2025/8/9 08:50
 * @description:
 */
public class MapSuppTsFunc extends RichMapFunction<JSONObject,JSONObject> {
    @Override
    public JSONObject map(JSONObject data) throws Exception {

        if(data != null && data.containsKey("upexgmsgreallocation")){
            JSONObject upexg = data.getJSONObject("upexgmsgreallocation");
            if (upexg != null && !upexg.isEmpty()){
                Object lon = upexg.getOrDefault("lon", "");
                Object lat = upexg.getOrDefault("lat","");
                Object msgId = upexg.getOrDefault("msgId","");
                Object direction = upexg.getOrDefault("direction","");
                Object dateTime = upexg.getOrDefault("dateTime","");

                Object vec1 = upexg.getOrDefault("vec1", "");
                Object vec2 = upexg.getOrDefault("vec2", "");
                Object vec3 = upexg.getOrDefault("vec3", "");
                Object encrypy = upexg.getOrDefault("encrypy", "");
                Object altitude = upexg.getOrDefault("altitude", "");
                Object alarm = upexg.getOrDefault("alarm", "");
                Object state = upexg.getOrDefault("state", "");

                data.put("vec1",vec1);
                data.put("vec2",vec2);
                data.put("vec3",vec3);
                data.put("encrypy",encrypy);
                data.put("altitude",altitude);
                data.put("alarm",alarm);
                data.put("state",state);
                data.put("lon",lon);
                data.put("lat",lat);
                data.put("msgId",msgId);
                data.put("direction",direction);
                data.put("dateTime",dateTime);
                data.put("op","r");
                data.put("ds",convertToDateDs((String) dateTime));
                data.put("ts",convertToTimestamp((String) dateTime));
            }
        }

        if (data != null && data.containsKey("upexgmsgregister")){
            JSONObject upexgmsgregister = data.getJSONObject("upexgmsgregister");
            if(upexgmsgregister != null && !upexgmsgregister.isEmpty()){
                data.put("upexgmsgregister",upexgmsgregister);
                data.put("ds","99991231");
            }
        }

        if (data != null) {
            data.remove("upexgmsgreallocation");
        }

        return data;
    }

    public static String convertToDateDs(String dateTimeStr) {
        if (dateTimeStr == null || dateTimeStr.length() < 8) {
            return "";
        }
        return dateTimeStr.substring(0, 8);
    }

    public static long convertToTimestamp(String dateTimeStr) {
        if (dateTimeStr == null || dateTimeStr.length() != 14) {
            return 0;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));

        try {
            Date date = sdf.parse(dateTimeStr);
            return date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return 0;
        }
    }
}
