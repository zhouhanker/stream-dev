package com.v3;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.v3.TestPgCDC
 * @Author zhou.han
 * @Date 2025/7/23 10:59
 * @description:
 */
public class TestPgCDC {
    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        DebeziumDeserializationSchema<String> deserializer = new JsonDebeziumDeserializationSchema();
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("10.160.60.14")
                        .port(5432)
                        .database("spider_db")
                        .schemaList("public")
                        .tableList("public.source_data_car_info_message_dtl")
                        .username("etl_flink_cdc_pub_user")
                        .password("etl_flink_cdc_pub_user123,./")
                        .slotName("flink_etl_cdc_test")
                        .deserializer(deserializer)
                        .decodingPluginName("pgoutput")
                        .includeSchemaChanges(true)
                        .startupOptions(StartupOptions.initial())
                        .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);

        env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(2)
                .print();


        env.execute();
    }
}
