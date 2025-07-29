package com.v3;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;


/**
 * @Package com.v3.TestSqlServerCDC
 * @Author zhou.han
 * @Date 2025/7/23 15:08
 * @description:
 */
public class TestSqlServerCDC {

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "schema_only");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("10.160.60.19")
                .port(1433)
                .username("sa")
                .password("zh1028,./")
                .database("realtime_v3")
                .tableList("dbo.cdc_test")
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");
        dataStreamSource.print().setParallelism(1);
        env.execute("sqlserver-cdc-test");

    }


    public static Properties getDebeziumProperties() {
        Properties properties = new Properties();
        properties.put("converters", "sqlserverDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.type", "SqlserverDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.database.type", "sqlserver");
        // 自定义格式，可选
        properties.put("sqlserverDebeziumConverter.format.datetime", "yyyy-MM-dd HH:mm:ss");
        properties.put("sqlserverDebeziumConverter.format.date", "yyyy-MM-dd");
        properties.put("sqlserverDebeziumConverter.format.time", "HH:mm:ss");
        return properties;
    }

    /*
     *      i {"before":null,"after":{"id":9,"name":"jjj","ts":1753346775537},"source":{"version":"1.9.7.Final","connector":"sqlserver","name":"sqlserver_transaction_log_source","ts_ms":1753317975547,"snapshot":"false","db":"realtime_v3","sequence":null,"schema":"dbo","table":"cdc_test","change_lsn":"0000002b:000002e3:0002","commit_lsn":"0000002b:000002e3:0003","event_serial_no":1},"op":"c","ts_ms":1753317976768,"transaction":null}
     *      d {"before":{"id":9,"name":"jjj","ts":1753346775537},"after":null,"source":{"version":"1.9.7.Final","connector":"sqlserver","name":"sqlserver_transaction_log_source","ts_ms":1753318054923,"snapshot":"false","db":"realtime_v3","sequence":null,"schema":"dbo","table":"cdc_test","change_lsn":"0000002b:000002f6:0002","commit_lsn":"0000002b:000002f6:0003","event_serial_no":1},"op":"d","ts_ms":1753318056689,"transaction":null}
     *      u {"before":{"id":5,"name":"gggr","ts":1753286645690},"after":{"id":5,"name":"eee","ts":1753286645690},"source":{"version":"1.9.7.Final","connector":"sqlserver","name":"sqlserver_transaction_log_source","ts_ms":1753318141440,"snapshot":"false","db":"realtime_v3","sequence":null,"schema":"dbo","table":"cdc_test","change_lsn":"0000002b:0000030a:0002","commit_lsn":"0000002b:0000030a:0003","event_serial_no":2},"op":"u","ts_ms":1753318141695,"transaction":null}
     */

}
