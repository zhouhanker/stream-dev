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
import org.apache.hadoop.hbase.util.MD5Hash;


import java.nio.charset.StandardCharsets;
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

        System.err.println(MD5Hash.getMD5AsHex("15".getBytes(StandardCharsets.UTF_8)));



    }
}
