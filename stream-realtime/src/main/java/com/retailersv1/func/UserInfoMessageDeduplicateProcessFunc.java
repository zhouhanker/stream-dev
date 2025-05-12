package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @Package com.retailersv1.func.UserInfoMessageDeduplicateProcessFunc
 * @Author zhou.han
 * @Date 2025/5/12 13:57
 * @description: UserInfo 数据去重
 */
public class UserInfoMessageDeduplicateProcessFunc extends KeyedProcessFunction<Long, JSONObject,JSONObject> {

    private ValueState<Long> latestTsState;

    @Override
    public void open(Configuration parameters) {
        // 定义状态并设置 TTL（1天过期）
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();

        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "latestTs",
                Long.class
        );
        descriptor.enableTimeToLive(ttlConfig);
        latestTsState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(JSONObject jsonObject, KeyedProcessFunction<Long, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        Long currentTs = jsonObject.getLong("ts_ms");
        Long latestTs = latestTsState.value();

        // 仅当当前记录的 ts_ms 大于状态中的 ts_ms 时，输出并更新状态
        if (latestTs == null || currentTs > latestTs) {
            collector.collect(jsonObject);
            latestTsState.update(currentTs);
        }
    }
}
