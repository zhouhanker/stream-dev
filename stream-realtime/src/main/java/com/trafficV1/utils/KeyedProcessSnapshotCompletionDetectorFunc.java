package com.trafficV1.utils;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 * @Package com.trafficV1.utils.KeyedProcessSnapshotCompletionDetectorFunc
 * @Author zhou.han
 * @Date 2025/8/14 14:55
 * @description: 检测cdc状态为r的快照数据是否完成
 */
public class KeyedProcessSnapshotCompletionDetectorFunc extends KeyedProcessFunction<String, JSONObject,JSONObject> {

    private static final Logger logger = LoggerFactory.getLogger(KeyedProcessSnapshotCompletionDetectorFunc.class);
    // 15秒无数据超时
    private static final long INACTIVITY_TIMEOUT = 15 * 1000;
    private ValueState<Boolean> snapshotCompletedState;
    private ValueState<Long> lastActivityTimeState;     // 记录最后一条数据的时间
    private ValueState<Boolean> isSnapshotPhaseState;   // 标记是否处于快照阶段


    @Override
    public void open(Configuration parameters) throws IOException {

        @SuppressWarnings("deprecation")
        ValueStateDescriptor<Boolean> snapshotCompletedDescriptor = new ValueStateDescriptor<>("snapshotCompleted", Boolean.class,false);
        @SuppressWarnings("deprecation")
        ValueStateDescriptor<Long> lastActivityTimeDescriptor = new ValueStateDescriptor<>("lastActivityTime", Long.class,0L);
        @SuppressWarnings("deprecation")
        ValueStateDescriptor<Boolean> isSnapshotPhaseDescriptor = new ValueStateDescriptor<>("isSnapshotPhase", Boolean.class,false);

        snapshotCompletedState = getRuntimeContext().getState(snapshotCompletedDescriptor);
        lastActivityTimeState = getRuntimeContext().getState(lastActivityTimeDescriptor);
        isSnapshotPhaseState = getRuntimeContext().getState(isSnapshotPhaseDescriptor);

    }
    @Override
    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        long currentTime = System.currentTimeMillis();
        lastActivityTimeState.update(currentTime);
        // 先删除旧定时器，再注册新定时器
        long oldTimer = lastActivityTimeState.value() + INACTIVITY_TIMEOUT;
        ctx.timerService().deleteProcessingTimeTimer(oldTimer);
        ctx.timerService().registerProcessingTimeTimer(currentTime + INACTIVITY_TIMEOUT);

        JSONObject source = value.getJSONObject("source");
        String op = value.getString("op");
        boolean isSnapshotData = "r".equals(op) || (source != null && "true".equals(source.getString("snapshot")));

        if (isSnapshotData) {
            out.collect(value);
            isSnapshotPhaseState.update(true);
        } else {
            // 用默认值false判断，避免null
            if (!snapshotCompletedState.value()) {
                markSnapshotCompleted(ctx);
            }
            isSnapshotPhaseState.update(false);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
        // 使用状态的当前值（已通过open设置默认值，不会为null）
        long lastActivityTime = lastActivityTimeState.value();
        boolean isSnapshotPhase = isSnapshotPhaseState.value();
        boolean snapshotCompleted = snapshotCompletedState.value();

        // 检查是否超时且处于快照阶段且未完成
        if (timestamp >= lastActivityTime + INACTIVITY_TIMEOUT && isSnapshotPhase && !snapshotCompleted) {
            markSnapshotCompleted(ctx);
        }
    }

    private void markSnapshotCompleted(@NotNull Context ctx) throws Exception {
        snapshotCompletedState.update(true);
        isSnapshotPhaseState.update(false);

        String message = "CDC Snapshot COMPLETED at " + new Date();
        System.err.println(message);
        logger.info(message);

        ctx.timerService().deleteProcessingTimeTimer(lastActivityTimeState.value() + INACTIVITY_TIMEOUT);
    }
}
