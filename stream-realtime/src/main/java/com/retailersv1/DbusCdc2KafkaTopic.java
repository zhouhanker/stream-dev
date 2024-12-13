package com.retailersv1;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.retailersv1.DbusCdc2KafkaTopic
 * @Author zhou.han
 * @Date 2024/12/12 12:56
 * @description: mysql db cdc to kafka realtime_db topic
 */
public class DbusCdc2KafkaTopic {

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySqlCdcSource = MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .databaseList(ConfigUtils.getString("mysql.database"))
                .tableList("")
                .username(ConfigUtils.getString("mysql.user"))
                .password(ConfigUtils.getString("mysql.pwd"))
                .serverTimeZone(ConfigUtils.getString("mysql.timezone"))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .includeSchemaChanges(true)
                .build();

        DataStreamSource<String> cdcStream = env.fromSource(mySqlCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_source");

        cdcStream.print();

        cdcStream.sinkTo(
                KafkaUtils.buildKafkaSink(ConfigUtils.getString("kafka.bootstrap.servers"),"realtime_v1_mysql_db")
        ).uid("sink_to_kafka_realtime_v1_mysql_db").name("sink_to_kafka_realtime_v1_mysql_db");





        env.execute();
    }

}
