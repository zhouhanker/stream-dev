package com.label;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.label.func.ProcessJoinBase2AndBase4Func;
import com.label.func.ProcessJoinBase6LabelFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.common.utils.WaterMarkUtils;
import lombok.SneakyThrows;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Date;

/**
 * @Package com.label.DbusUserLabel6BaseCalculate
 * @Author zhou.han
 * @Date 2025/5/15 15:32
 * @description: Task 02
 */
public class DbusUserLabel6BaseCalculate {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_label_base6_topic = ConfigUtils.getString("kafka.result.label.base6.topic");
    private static final String kafka_label_base4_topic = ConfigUtils.getString("kafka.result.label.base4.topic");
    private static final String kafka_label_base2_topic = ConfigUtils.getString("kafka.result.label.base2.topic");
    private static final String kafka_label_user_baseline_topic = ConfigUtils.getString("kafka.result.label.baseline.topic");

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        SingleOutputStreamOperator<String> kafkaBase6Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_label_base6_topic,
                                new Date()+"_kafkaBase6Source",
                                OffsetsInitializer.earliest()
                        ),
                        WaterMarkUtils.publicAssignWatermarkStrategy("ts_ms",3),
                        "kafka_label_base6_topic_source"
                ).uid("kafka_base6_source")
                .name("kafka_base6_source");

        SingleOutputStreamOperator<String> kafkaBase4Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_label_base4_topic,
                                new Date()+"_kafkaBase4Source",
                                OffsetsInitializer.earliest()
                        ),
                        WaterMarkUtils.publicAssignWatermarkStrategy("ts_ms",3),
                        "kafka_label_base4_topic_source"
                ).uid("kafka_base4_source")
                .name("kafka_base4_source");

        SingleOutputStreamOperator<String> kafkaBase2Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_label_base2_topic,
                                new Date()+"_kafkaBase2Source",
                                OffsetsInitializer.earliest()
                        ),
                        WaterMarkUtils.publicAssignWatermarkStrategy("ts_ms",3),
                        "kafka_label_base2_topic_source"
                ).uid("kafka_base2_source")
                .name("kafka_base2_source");

        SingleOutputStreamOperator<JSONObject> mapBase6LabelDs = kafkaBase6Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase4LabelDs = kafkaBase4Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase2LabelDs = kafkaBase2Source.map(JSON::parseObject);


        KeyedStream<JSONObject, String> keyedStreamBase2LabelDs = mapBase2LabelDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamBase4LabelDs = mapBase4LabelDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamBase6LabelDs = mapBase6LabelDs.keyBy(data -> data.getString("uid"));

        SingleOutputStreamOperator<JSONObject> processJoinBase2AndBase4LabelDs = keyedStreamBase2LabelDs.intervalJoin(keyedStreamBase4LabelDs)
                .between(Time.hours(-6), Time.hours(6))
                .process(new ProcessJoinBase2AndBase4Func());


        KeyedStream<JSONObject, String> keyedStreamJoinBase2AndBase4LabelDs = processJoinBase2AndBase4LabelDs.keyBy(data -> data.getString("uid"));


        SingleOutputStreamOperator<JSONObject> processUserLabelDs = keyedStreamJoinBase2AndBase4LabelDs.intervalJoin(keyedStreamBase6LabelDs)
                .between(Time.hours(-6), Time.hours(6))
                .process(new ProcessJoinBase6LabelFunc());


        processUserLabelDs.map(data -> data.toJSONString())
                .sinkTo(
                        KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_label_user_baseline_topic)
                );

        processUserLabelDs.print("processUserLabelDs -> ");




        env.execute();
    }

}
