package com.v3;
import lombok.SneakyThrows;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
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

        DebeziumDeserializationSchema<String> deserializer = new JsonDebeziumDeserializationSchema();
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("10.160.60.14")
                        .port(5432)
                        .database("spider_db")
                        .schemaList("public")
                        .tableList("public.test_cdc")
                        .username("etl_flink_cdc_pub_user")
                        .password("etl_flink_cdc_pub_user123,./")
                        .slotName("flink_etl_cdc_test1")
                        .deserializer(deserializer)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(3000);

        env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(2)
                .print();


        env.execute();
    }
}
