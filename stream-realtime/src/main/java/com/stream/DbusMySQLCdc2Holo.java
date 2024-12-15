package com.stream;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.ververica.connectors.common.sink.OutputFormatSinkFunction;
import com.alibaba.ververica.connectors.hologres.api.HologresRecordConverter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.config.HologresConfigs;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCWriter;
import com.alibaba.ververica.connectors.hologres.sink.HologresOutputFormat;
import com.stream.common.utils.ConfigUtils;
import com.stream.domain.MySQLMessageInfo;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;


/**
 * @Package com.zh.flk.task.FkMySQLCdc2Holo
 * @Author zhou.han
 * @Date 2024/10/10 16:01
 * @description: Listen MySQLBinLog Data 2 Holograms
 */
public class DbusMySQLCdc2Holo {

    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .databaseList(ConfigUtils.getString("mysql.database"))
                .tableList("business_dev.test.cdc")
                .username(ConfigUtils.getString("mysql.user"))
                .password(ConfigUtils.getString("mysql.pwd"))
                .serverTimeZone(ConfigUtils.getString("mysql.timezone"))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .includeSchemaChanges(true)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source-cdc-listen");
        dataStreamSource.print();
        SingleOutputStreamOperator<JSONObject> map = dataStreamSource.map(JSONObject::parseObject);

        SingleOutputStreamOperator<MySQLMessageInfo> res = map.map(json -> {
            MySQLMessageInfo mySQLMessageInfo = new MySQLMessageInfo();
            mySQLMessageInfo.setId(json.getString("id"));
            mySQLMessageInfo.setOp(json.getString("op"));
            mySQLMessageInfo.setDb_name(json.getString("database"));
            mySQLMessageInfo.setLog_before(json.getString("before"));
            mySQLMessageInfo.setLog_after(json.getString("after"));
            mySQLMessageInfo.setT_name(json.getString("tableName"));
            mySQLMessageInfo.setTs(json.getString("ts"));
            return mySQLMessageInfo;
        });


        TableSchema tableSchema = TableSchema.builder()
                .field("id", DataTypes.STRING())
                .field("op", DataTypes.STRING())
                .field("db_name", DataTypes.STRING())
                .field("log_before", DataTypes.STRING())
                .field("log_after", DataTypes.STRING())
                .field("t_name", DataTypes.STRING())
                .field("ts", DataTypes.STRING())
                .build();

//        Schema tableSchema = Schema.newBuilder()
//                .column("id", DataTypes.STRING())
//                .column("op", DataTypes.STRING())
//                .column("db_name", DataTypes.STRING())
//                .column("log_before", DataTypes.STRING())
//                .column("log_after", DataTypes.STRING())
//                .column("id", DataTypes.STRING())
//                .column("t_name", DataTypes.STRING())
//                .column("ts", DataTypes.STRING())
//                .build();

        HologresConnectionParam hologresConnectionParam = hologresConfig();

        res.print();
        res.addSink(
                new OutputFormatSinkFunction<MySQLMessageInfo>(
                        new HologresOutputFormat<>(
                                hologresConnectionParam,
                                new HologresJDBCWriter<>(
                                        hologresConnectionParam,
                                        tableSchema,
                                        new RecordConverter(hologresConnectionParam)
                                ))
                )
        );




//        env.disableOperatorChaining();
//        env.execute();
    }

    private static HologresConnectionParam hologresConfig(){
        Configuration configuration = new Configuration();
        configuration.set(HologresConfigs.ENDPOINT,ConfigUtils.getString("holo.endpoint"));
        configuration.set(HologresConfigs.DATABASE,ConfigUtils.getString("holo.database"));
        configuration.set(HologresConfigs.USERNAME,ConfigUtils.getString("ali.key"));
        configuration.set(HologresConfigs.PASSWORD,ConfigUtils.getString("ali.pwd"));
        configuration.set(HologresConfigs.TABLE,"public.mysql_binlog_info");
        configuration.set(HologresConfigs.MUTATE_TYPE,"insertorupdate");
        configuration.set(HologresConfigs.OPTIONAL_SINK_IGNORE_DELETE,false);
        configuration.setBoolean(HologresConfigs.CREATE_MISSING_PARTITION_TABLE, true);

        return new HologresConnectionParam(configuration);
    }

    public static class RecordConverter implements HologresRecordConverter<MySQLMessageInfo, Record> {

        private final HologresConnectionParam hologresConnectionParam;
        private HologresTableSchema tableSchema;

        public RecordConverter(HologresConnectionParam hologresConnectionParam) {
            this.hologresConnectionParam = hologresConnectionParam;
        }

        @Override
        public Record convertFrom(MySQLMessageInfo message) {
            if (tableSchema == null) {
                this.tableSchema =
                        HologresTableSchema.get(hologresConnectionParam.getJdbcOptions());
            }

            System.err.println("------data start------");
            System.err.println(tableSchema.toString());
            System.err.println(message);
            System.err.println("------data end------");

            Record result = new Record(tableSchema.get());
            result.setObject(0, message.getId());
            result.setObject(1, message.getOp());
            result.setObject(2, message.getDb_name());
            result.setObject(3, message.getLog_before());
            result.setObject(4, message.getLog_after());
            result.setObject(5, message.getT_name());
            result.setObject(6, message.getTs());

            System.err.println("result -> "+result);


            return result;
        }

        @Override
        public MySQLMessageInfo convertTo(Record record) {
             throw new UnsupportedOperationException("No need to implement");
        }

        @Override
        public Record convertToPrimaryKey(MySQLMessageInfo jsonObject) {
            throw new UnsupportedOperationException("No need to implement");
        }
    }





}
