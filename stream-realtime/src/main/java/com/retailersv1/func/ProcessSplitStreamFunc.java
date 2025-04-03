package com.retailersv1.func;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Package com.retailersv1.func.ProcessSplitStream
 * @Author zhou.han
 * @Date 2024/12/24 11:58
 * @description:
 */
public class ProcessSplitStreamFunc extends ProcessFunction<JSONObject,String> {

    private OutputTag<String> errTag;
    private OutputTag<String> startTag ;
    private OutputTag<String> displayTag ;
    private OutputTag<String> actionTag ;

    public ProcessSplitStreamFunc(OutputTag<String> errTag, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        this.startTag = startTag;
        this.errTag = errTag;
        this.actionTag = actionTag;
        this.displayTag = displayTag;
    }

    @Override
    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
        JSONObject errJson = jsonObject.getJSONObject("err");
        if (errJson != null){
            context.output(errTag, errJson.toJSONString());
            jsonObject.remove("err");
        }
        JSONObject startJsonObj = jsonObject.getJSONObject("start");
        if (startJsonObj != null) {
            context.output(startTag, jsonObject.toJSONString());
        }else {
            JSONObject commonJsonObj = jsonObject.getJSONObject("common");
            JSONObject pageJsonObj = jsonObject.getJSONObject("page");
            Long ts = jsonObject.getLong("ts");
            JSONArray displayArr = jsonObject.getJSONArray("displays");
            if (displayArr != null && displayArr.size() > 0) {
                //遍历当前页面的所有曝光信息
                for (int i = 0; i < displayArr.size(); i++) {
                    JSONObject disPlayJsonObj = displayArr.getJSONObject(i);
                    //定义一个新的JSON对象，用于封装遍历出来的曝光数据
                    JSONObject newDisplayJsonObj = new JSONObject();
                    newDisplayJsonObj.put("common", commonJsonObj);
                    newDisplayJsonObj.put("page", pageJsonObj);
                    newDisplayJsonObj.put("display", disPlayJsonObj);
                    newDisplayJsonObj.put("ts", ts);
                    //将曝光日志写到曝光侧输出流
                    context.output(displayTag, newDisplayJsonObj.toJSONString());
                }
                jsonObject.remove("displays");
            }
            JSONArray actionArr = jsonObject.getJSONArray("actions");
            if (actionArr != null && actionArr.size() > 0) {
                //遍历出每一个动作
                for (int i = 0; i < actionArr.size(); i++) {
                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                    //定义一个新的JSON对象，用于封装动作信息
                    JSONObject newActionJsonObj = new JSONObject();
                    newActionJsonObj.put("common", commonJsonObj);
                    newActionJsonObj.put("page", pageJsonObj);
                    newActionJsonObj.put("action", actionJsonObj);
                    //将动作日志写到动作侧输出流
                    context.output(actionTag, newActionJsonObj.toJSONString());
                }
                jsonObject.remove("actions");
            }
            collector.collect(jsonObject.toJSONString());
        }
    }
}
