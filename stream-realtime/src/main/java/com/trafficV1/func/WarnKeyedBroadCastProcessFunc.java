package com.trafficV1.func;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Package com.trafficV1.func.WarnKeyedBroadCastProcessFunc
 * @Author zhou.han
 * @Date 2025/8/22 09:59
 * @description:
 */
public class WarnKeyedBroadCastProcessFunc extends KeyedBroadcastProcessFunction<String, JSONObject,List<String>,JSONObject> {

    private static final Logger logger = LoggerFactory.getLogger(WarnKeyedBroadCastProcessFunc.class);

    private final OutputTag<String> emergencyAlarmTag;
    private final OutputTag<String> vehiclenoAlarmTag;

    private final MapStateDescriptor<Void, List<String>> configDescriptor;
    public WarnKeyedBroadCastProcessFunc(OutputTag<String> emergencyAlarmTag, OutputTag<String> vehiclenoAlarmTag, MapStateDescriptor<Void, List<String>> warnConfigMapStateDesc) {
        this.emergencyAlarmTag = emergencyAlarmTag;
        this.vehiclenoAlarmTag = vehiclenoAlarmTag;
        this.configDescriptor = warnConfigMapStateDesc;
    }

    @Override
    public void processElement(JSONObject jsonObject, KeyedBroadcastProcessFunction<String, JSONObject, List<String>, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<Void, List<String>> broadcastState = readOnlyContext.getBroadcastState(configDescriptor);
        List<String> listConfig = broadcastState.get(null);
        collector.collect(jsonObject);
        if (listConfig != null && !listConfig.isEmpty()){
            for (String rule : listConfig) {
                try {
                    JSONObject ruleJson = JSON.parseObject(rule);
                    processStreamRules(ruleJson,jsonObject,readOnlyContext);
                }catch (Exception e){
                    logger.warn("Failed to parse or apply rule: {}", rule, e);
                }
            }
        }
    }

    @Override
    public void processBroadcastElement(List<String> config, KeyedBroadcastProcessFunction<String, JSONObject, List<String>, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        BroadcastState<Void, List<String>> broadcastState = context.getBroadcastState(configDescriptor);
        broadcastState.put(null, config);
        logger.info("配置已更新，共{}条规则", config.size());
    }

    public void processStreamRules(JSONObject rule, JSONObject data, ReadOnlyContext context){
        String ruleType = rule.getString("rule_type");
        if (ruleType != null && !rule.isEmpty()){
            switch (ruleType){
                case "1":
                    handleEmergencyAlarmRule(data, context);
                case "2":
                    handleVehiclenoRule(rule, data, context);
                    break;
                default:
                    logger.warn("Unknown rule type: {}", ruleType);
                    break;
            }
        }
    }

    private void handleEmergencyAlarmRule(JSONObject data, ReadOnlyContext context) {
        JSONObject alarm = data.getJSONObject("alarm");
        if (alarm != null) {
            Integer emergencyAlarm = alarm.getInteger("emergencyAlarm");
            if (emergencyAlarm != null && emergencyAlarm == 1) {
                context.output(emergencyAlarmTag, data.toJSONString());
                logger.info("Emergency alarm rule matched for data: {}", data.toJSONString());
            }
        }
    }

    private void handleVehiclenoRule(JSONObject rule, JSONObject data, ReadOnlyContext context) {
        String ruleVehicleno = rule.getString("vehicleno");
        String dataVehicleno = data.getString("vehicleno");

        if (ruleVehicleno != null && ruleVehicleno.equals(dataVehicleno)) {
            context.output(vehiclenoAlarmTag, data.toJSONString());
            logger.debug("Vehicleno rule matched for data: {}", data.toJSONString());
        }
    }

}
