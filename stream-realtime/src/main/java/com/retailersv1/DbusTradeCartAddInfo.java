package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateTimeUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.PaimonMinioUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Date;


/**
 * @Package com.retailersv1.DbusTradeCartAddInfo
 * @Author zhou.han
 * @Date 2025/4/7 21:11
 * @description: Task 05
 */
public class DbusTradeCartAddInfo {

    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_cdc_db_topic = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String catalog_minio_name = "minio_paimon_catalog";
    private static final String minio_database_name = "realtime_v2";

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "NONE");

        PaimonMinioUtils.ExecCreateMinioCatalogAndDatabases(tenv,catalog_minio_name,minio_database_name);

        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        kafka_cdc_db_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> JSONObject.parseObject(event).getLong("ts_ms")),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");

        SingleOutputStreamOperator<JSONObject> cartInfoDs = kafkaCdcDbSource.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("cart_info"))
                .uid("filter cart_info data")
                .name("filter cart_info data");


        SingleOutputStreamOperator<JSONObject> filterOpDs = cartInfoDs.filter(data -> data.getString("op").equals("u") || data.getString("op").equals("c"))
                .uid("filter op model")
                .name("filter op model");

        SingleOutputStreamOperator<JSONObject> fixDs = filterOpDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                JSONObject resJson = new JSONObject();
                resJson.put("ts_ms", jsonObject.getLong("ts_ms"));
                resJson.put("is_ordered", jsonObject.getJSONObject("after").getIntValue("is_ordered"));
                resJson.put("sku_num", jsonObject.getJSONObject("after").getIntValue("sku_num"));
                resJson.put("create_time", jsonObject.getJSONObject("after").getLong("create_time"));
                resJson.put("user_id", jsonObject.getJSONObject("after").getString("user_id"));
                resJson.put("sku_id", jsonObject.getJSONObject("after").getIntValue("sku_id"));
                resJson.put("sku_name", jsonObject.getJSONObject("after").getString("sku_name"));
                resJson.put("id", jsonObject.getJSONObject("after").getIntValue("id"));
                resJson.put("operate_time", jsonObject.getJSONObject("after").getLong("operate_time"));
                resJson.put("ds", DateTimeUtils.format(new Date(jsonObject.getLong("ts_ms")), "yyyyMMdd"));
                return resJson;
            }
        }).uid("fix data").name("fix data");

        Schema schema = Schema.newBuilder()
                .column("ts_ms", DataTypes.BIGINT())
                .column("is_ordered", DataTypes.INT())
                .column("sku_num", DataTypes.INT())
                .column("create_time", DataTypes.BIGINT())
                .column("user_id", DataTypes.STRING())
                .column("sku_id", DataTypes.INT())
                .column("sku_name", DataTypes.STRING())
                .column("id", DataTypes.INT())
                .column("operate_time", DataTypes.BIGINT())
                .column("ds", DataTypes.STRING())
                .build();

        TypeInformation<Row> rowTypeInfo = Types.ROW_NAMED(
                new String[]{"ts_ms", "is_ordered", "sku_num", "create_time", "user_id", "sku_id", "sku_name", "id", "operate_time", "ds"},
                Types.LONG,
                Types.INT,
                Types.INT,
                Types.LONG,
                Types.STRING,
                Types.INT,
                Types.STRING,
                Types.INT,
                Types.LONG,
                Types.STRING
        );

        SingleOutputStreamOperator<Row> rowDs = fixDs.map((MapFunction<JSONObject, Row>) json -> {
            Row row = Row.withNames();
            row.setField("ts_ms", json.getLong("ts_ms"));
            row.setField("is_ordered", json.getIntValue("is_ordered"));
            row.setField("sku_num", json.getIntValue("sku_num"));
            row.setField("create_time", json.getLong("create_time"));
            row.setField("user_id", json.getString("user_id"));
            row.setField("sku_id", json.getIntValue("sku_id"));
            row.setField("sku_name", json.getString("sku_name"));
            row.setField("id", json.getIntValue("id"));
            row.setField("operate_time", json.getLong("operate_time"));
            row.setField("ds", json.getString("ds"));
            return row;
        }).returns(rowTypeInfo);


        tenv.createTemporaryView("flk_res_cart_info_tle", tenv.fromChangelogStream(rowDs, schema));

//        tenv.executeSql("drop table if exists realtime_v2.res_cart_info_tle;");
        tenv.executeSql("CREATE TABLE if not exists realtime_v2.res_cart_info_tle ( \n" +
                "  ts_ms bigint,                                                      \n" +
                "  is_ordered int,                                                    \n" +
                "  sku_num int,                                                       \n" +
                "  create_time bigint,                                                \n" +
                "  user_id varchar(50),                                               \n" +
                "  sku_id bigint,                                                     \n" +
                "  sku_name varchar(255),                                             \n" +
                "  id int,                                                            \n" +
                "  operate_time bigint,                                               \n" +
                "  ds varchar(255),                                                   \n" +
                "  PRIMARY KEY (id,ds) NOT ENFORCED                                   \n" +
                ")                                                                    \n" +
                "partitioned by(ds)                                                   \n" +
                "WITH                                                                 \n" +
                "(                                                                    \n" +
                "  'deletion-vectors.enabled' = 'true',                               \n" +
                "  'bucket' = '2',                                                    \n" +
                "  'sequence.field' = 'ts_ms',                                        \n" +
                "  'changelog-producer' = 'LOOKUP',                                     \n" +
                "  'changelog-producer.compaction-interval' = '1 min',                \n" +
                "  'merge-engine' = 'partial-update',                                 \n" +
                "  'partial-update.ignore-delete' = 'true'                            \n" +
                ");");



        tenv.executeSql(
                "insert into realtime_v2.res_cart_info_tle   \n" +
                   "select *                                    \n" +
                   "from flk_res_cart_info_tle;"
        );


    }
}
