package com.label.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator5.com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * @Package com.retailersv1.func.FilterBloomDeduplicatorFunc
 * @Author zhou.han
 * @Date 2025/3/31 22:25
 * @description: 布隆过滤器
 */
public class FilterBloomDeduplicatorPublicFunc extends RichFilterFunction<JSONObject> {

    private static final Logger logger = LoggerFactory.getLogger(FilterBloomDeduplicatorPublicFunc.class);

    private final int expectedInsertions;
    private final double falsePositiveRate;
    private String sortKey1;
    private String sortKey2;
    private transient ValueState<byte[]> bloomState;

    public FilterBloomDeduplicatorPublicFunc(int expectedInsertions, double falsePositiveRate,String sortKey1,String sortKey2) {
        this.expectedInsertions = expectedInsertions;
        this.falsePositiveRate = falsePositiveRate;
        this.sortKey1 = sortKey1;
        this.sortKey2 = sortKey2;
    }

    @Override
    public void open(Configuration parameters){
        ValueStateDescriptor<byte[]> descriptor = new ValueStateDescriptor<>(
                "bloomFilterState",
                BytePrimitiveArraySerializer.INSTANCE
        );

        bloomState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public boolean filter(JSONObject value) throws Exception {
        long orderId = value.getLong(sortKey1);
        long tsMs = value.getLong(sortKey2);
        String compositeKey = orderId + "_" + tsMs;

        // 读取状态
        byte[] bitArray = bloomState.value();
        if (bitArray == null) {
            bitArray = new byte[(optimalNumOfBits(expectedInsertions, falsePositiveRate) + 7) / 8];
        }

        boolean mightContain = true;
        int hash1 = hash(compositeKey);
        int hash2 = hash1 >>> 16;

        for (int i = 1; i <= optimalNumOfHashFunctions(expectedInsertions, bitArray.length * 8L); i++) {
            int combinedHash = hash1 + (i * hash2);
            if (combinedHash < 0) combinedHash = ~combinedHash;
            int pos = combinedHash % (bitArray.length * 8);

            int bytePos = pos / 8;
            int bitPos = pos % 8;
            byte current = bitArray[bytePos];

            if ((current & (1 << bitPos)) == 0) {
                mightContain = false;
                bitArray[bytePos] = (byte) (current | (1 << bitPos));
            }
        }

        // 如果是新数据，更新状态并保留
        if (!mightContain) {
            bloomState.update(bitArray);
            return true;
        }

        // 可能重复的数据，过滤
        logger.warn("check duplicate data : {}", value);
        return false;
    }

    private int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    private int optimalNumOfBits(long n, double p) {
        if (p == 0) p = Double.MIN_VALUE;
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private int hash(String key) {
        return Hashing.murmur3_128().hashString(key, StandardCharsets.UTF_8).asInt();
    }
}
