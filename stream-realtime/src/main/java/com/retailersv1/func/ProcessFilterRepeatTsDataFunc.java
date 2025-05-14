package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

/**
 * @Package com.retailersv1.func.ProcessFullDataDeduplication
 * @Author
 * @Date
 * @description: 对完整数据进行去重
 */
public class ProcessFilterRepeatTsDataFunc extends KeyedProcessFunction<String, JSONObject, JSONObject> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessFilterRepeatTsDataFunc.class);
    private ValueState<HashSet<String>> processedDataState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<HashSet<String>> descriptor = new ValueStateDescriptor<>(
                "processedDataState",
                TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<HashSet<String>>() {})
        );
        processedDataState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        HashSet<String> processedData = processedDataState.value();
        if (processedData == null) {
            processedData = new HashSet<>();
        }

        String dataStr = value.toJSONString();
        LOG.info("Processing data: {}", dataStr);
        if (!processedData.contains(dataStr)) {
            LOG.info("Adding new data to set: {}", dataStr);
            processedData.add(dataStr);
            processedDataState.update(processedData);
            out.collect(value);
        } else {
            LOG.info("Duplicate data found: {}", dataStr);
        }
    }
}