package com.stream;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.domain.MySQLMessageInfo;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.Date;

/**
 * @Package com.stream.StreamTest
 * @Author zhou.han
 * @Date 2024/10/11 14:28
 * @description: Test
 */
public class StreamTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource("cdh01:9092", "realtime_log", new Date().toString(), OffsetsInitializer.earliest());

        DataStreamSource<String> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");
        dataStreamSource.print();




        env.execute();
    }
}
