package com.stream.common.utils;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @Package com.retailersv1.CustomStringSerializationSchema
 * @Author zhou.han
 * @Date 2024/12/14 20:10
 * @description: 1
 */
public class CustomStringSerializationSchema implements KafkaSerializationSchema<String> {
    private final String topic;

    public CustomStringSerializationSchema(String topic) {
        this.topic = topic;
    }
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
        return new ProducerRecord<>(topic, null, s.getBytes(StandardCharsets.UTF_8));
    }
}
