package com.trafficV1;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.trafficV1.func.MapSuppTsFunc;
import com.trafficV1.utils.KeyedProcessSnapshotCompletionDetectorFunc;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Objects;

/**
 * @Package com.trafficV1.DbusLoadingPg2Kafka
 * @Author zhou.han
 * @Date 2025/8/7 20:43
 * @description: Loading Postgresql & Task 01
 */
public class DbusLoadingPg2Kafka {

    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_topic_traffic_car_info = "realtime_v3_traffic_origin_data_info";
    private static final int kafka_topic_partition_nums = 6;
    private static final short kafka_topic_replication_nums = 1;

    @SneakyThrows
    public static void main(String[] args) {

        boolean kafkaTopicDelFlag = KafkaUtils.kafkaTopicExists(kafka_botstrap_servers, kafka_topic_traffic_car_info);
        KafkaUtils.createKafkaTopic(kafka_botstrap_servers,kafka_topic_traffic_car_info,kafka_topic_partition_nums,kafka_topic_replication_nums,kafkaTopicDelFlag);
        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("10.160.60.14")
                        .port(5432)
                        .database("spider_db")
                        .schemaList("public")
                        .tableList("public.source_data_car_info_message_dtl")
                        .username("etl_flink_cdc_pub_user")
                        .password("etl_flink_cdc_pub_user123,./")
                        .slotName("slot_read_pg_cdc_data_source_flk")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .decodingPluginName("pgoutput")
                        .includeSchemaChanges(true)
                        .startupOptions(StartupOptions.initial())
                        .build();

        DataStreamSource<String> pgCdcSource = env.fromSource(postgresIncrementalSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "DbusLoadingPgDataETL_postgresIncrementalSource");

        SingleOutputStreamOperator<JSONObject> mapData2JsonDs = pgCdcSource.map(JSON::parseObject)
                .uid("map_source_data_to_json")
                .name("_map_source_data_to_json");

        SingleOutputStreamOperator<JSONObject> processSnapshotDs = mapData2JsonDs.keyBy(r -> "static_key").process(new KeyedProcessSnapshotCompletionDetectorFunc()).setParallelism(1);

        SingleOutputStreamOperator<JSONObject> hisAfterDataNotNullDs = processSnapshotDs.filter(data -> "r".equals(data.getString("op")))
                .filter(Objects::nonNull)
                .map(data -> data.getJSONObject("after"))
                .uid("fm_json_data_not_null & get after data")
                .name("_fm_json_data_not_null & get after data");

        SingleOutputStreamOperator<JSONObject> hisFullDataDs = hisAfterDataNotNullDs.map(new MapSuppTsFunc())
                .uid("map_supp_map_history_data")
                .name("_map_supp_map_history_data");

        SingleOutputStreamOperator<String> hisFullData2StrRes = hisFullDataDs.map(JSONObject::toJSONString)
                .uid("map_convert_json_2_str")
                .name("_map_convert_json_2_str");

        hisFullData2StrRes.sinkTo(
                KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_topic_traffic_car_info)
        ).uid("sink_2_kf_topic").name("_sink_2_kf_topic");

//        hisFullData2StrRes.print();



        env.disableOperatorChaining();
        env.execute();
    }

}
