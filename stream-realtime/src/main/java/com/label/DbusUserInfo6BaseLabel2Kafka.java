package com.label;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.label.func.*;
import com.retailersv1.func.*;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.JdbcUtils;
import com.stream.common.utils.KafkaUtils;
import com.label.domain.DimBaseCategory;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;


/**
 * @Package com.label.DbusUserInfo6BaseLabel
 * @Author zhou.han
 * @Date 2025/5/12 10:01
 * @description: 01 Task 6 BaseLine
 */

public class DbusUserInfo6BaseLabel2Kafka {

    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_cdc_db_topic = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String kafka_page_log_topic = ConfigUtils.getString("kafka.page.topic");

    private static final String kafka_label_base6_topic = ConfigUtils.getString("kafka.result.label.base6.topic");
    private static final String kafka_label_base4_topic = ConfigUtils.getString("kafka.result.label.base4.topic");
    private static final String kafka_label_base2_topic = ConfigUtils.getString("kafka.result.label.base2.topic");

    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数
    private static final double time_rate_weight_coefficient = 0.1;    // 时间权重系数
    private static final double amount_rate_weight_coefficient = 0.15;    // 价格权重系数
    private static final double brand_rate_weight_coefficient = 0.2;    // 品牌权重系数
    private static final double category_rate_weight_coefficient = 0.3; // 类目权重系数

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    ConfigUtils.getString("mysql.url"),
                    ConfigUtils.getString("mysql.user"),
                    ConfigUtils.getString("mysql.pwd"));
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from realtime_v1.base_category3 as b3  \n" +
                    "     join realtime_v1.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join realtime_v1.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(4);

        // user info cdc
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        kafka_botstrap_servers,
                        kafka_cdc_db_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                            JSONObject jsonObject = JSONObject.parseObject(event);
                            if (event != null && jsonObject.containsKey("ts_ms")){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");

        // page log
        SingleOutputStreamOperator<String> kafkaPageLogSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        kafka_botstrap_servers,
                        kafka_page_log_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    JSONObject jsonObject = JSONObject.parseObject(event);
                                    if (event != null && jsonObject.containsKey("ts_ms")){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_page_log_source"
        ).uid("kafka_page_log_source")
         .name("kafka_page_log_source");

        SingleOutputStreamOperator<JSONObject> dataConvertJsonDs = kafkaCdcDbSource.map(JSON::parseObject)
                .uid("convert json cdc db")
                .name("convert json cdc db");

        SingleOutputStreamOperator<JSONObject> dataPageLogConvertJsonDs = kafkaPageLogSource.map(JSON::parseObject)
                .uid("convert json page log")
                .name("convert json page log");

        // 设备信息 + 关键词搜索
        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = dataPageLogConvertJsonDs.map(new MapDeviceInfoAndSearchKetWordMsgFunc())
                .uid("get device info & search")
                .name("get device info & search");


        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));


        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsDataFunc());

        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");


        // 设备打分模型 base2
        SingleOutputStreamOperator<JSONObject> mapDeviceAndSearchRateResultDs = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));



        SingleOutputStreamOperator<JSONObject> userInfoDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"))
                .uid("filter kafka user info")
                .name("filter kafka user info");

        SingleOutputStreamOperator<JSONObject> cdcOrderInfoDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"))
                .uid("filter kafka order info")
                .name("filter kafka order info");

        SingleOutputStreamOperator<JSONObject> cdcOrderDetailDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"))
                .uid("filter kafka order detail")
                .name("filter kafka order detail");

        SingleOutputStreamOperator<JSONObject> mapCdcOrderInfoDs = cdcOrderInfoDs.map(new MapOrderInfoDataFunc());
        SingleOutputStreamOperator<JSONObject> mapCdcOrderDetailDs = cdcOrderDetailDs.map(new MapOrderDetailFunc());

        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderInfoDs = mapCdcOrderInfoDs.filter(data -> data.getString("id") != null && !data.getString("id").isEmpty());
        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderDetailDs = mapCdcOrderDetailDs.filter(data -> data.getString("order_id") != null && !data.getString("order_id").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamCdcOrderInfoDs = filterNotNullCdcOrderInfoDs.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> keyedStreamCdcOrderDetailDs = filterNotNullCdcOrderDetailDs.keyBy(data -> data.getString("order_id"));

        SingleOutputStreamOperator<JSONObject> processIntervalJoinOrderInfoAndDetailDs = keyedStreamCdcOrderInfoDs.intervalJoin(keyedStreamCdcOrderDetailDs)
                .between(Time.minutes(-2), Time.minutes(2))
                .process(new IntervalDbOrderInfoJoinOrderDetailProcessFunc());

        SingleOutputStreamOperator<JSONObject> processDuplicateOrderInfoAndDetailDs = processIntervalJoinOrderInfoAndDetailDs.keyBy(data -> data.getString("detail_id"))
                .process(new processOrderInfoAndDetailFunc());

        // 品类 品牌 年龄 时间 base4
        SingleOutputStreamOperator<JSONObject> mapOrderInfoAndDetailModelDs = processDuplicateOrderInfoAndDetailDs.map(new MapOrderAndDetailRateModelFunc(dim_base_categories, time_rate_weight_coefficient, amount_rate_weight_coefficient, brand_rate_weight_coefficient, category_rate_weight_coefficient));

        SingleOutputStreamOperator<JSONObject> finalUserInfoDs = userInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                JSONObject after = jsonObject.getJSONObject("after");
                if (after != null && after.containsKey("birthday")) {
                    Integer epochDay = after.getInteger("birthday");
                    if (epochDay != null) {
                        LocalDate date = LocalDate.ofEpochDay(epochDay);
                        after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                    }
                }
                return jsonObject;
            }
        });



        SingleOutputStreamOperator<JSONObject> userInfoSupDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"))
                .uid("filter kafka user info sup")
                .name("filter kafka user info sup");

        SingleOutputStreamOperator<JSONObject> mapUserInfoDs = finalUserInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject){
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("uid", after.getString("id"));
                            result.put("uname", after.getString("name"));
                            result.put("user_level", after.getString("user_level"));
                            result.put("login_name", after.getString("login_name"));
                            result.put("phone_num", after.getString("phone_num"));
                            result.put("email", after.getString("email"));
                            result.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                            result.put("birthday", after.getString("birthday"));
                            result.put("ts_ms", jsonObject.getLongValue("ts_ms"));
                            String birthdayStr = after.getString("birthday");
                            if (birthdayStr != null && !birthdayStr.isEmpty()) {
                                try {
                                    LocalDate birthday = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
                                    LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                                    int age = calculateAge(birthday, currentDate);
                                    int decade = birthday.getYear() / 10 * 10;
                                    result.put("decade", decade);
                                    result.put("age", age);
                                    String zodiac = getZodiacSign(birthday);
                                    result.put("zodiac_sign", zodiac);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                        return result;
                    }
                })
                .uid("map userInfo ds")
                .name("map userInfo ds");

        SingleOutputStreamOperator<JSONObject> mapUserInfoSupDs = userInfoSupDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("uid", after.getString("uid"));
                            result.put("unit_height", after.getString("unit_height"));
                            result.put("create_ts", after.getLong("create_ts"));
                            result.put("weight", after.getString("weight"));
                            result.put("unit_weight", after.getString("unit_weight"));
                            result.put("height", after.getString("height"));
                            result.put("ts_ms", jsonObject.getLong("ts_ms"));
                        }
                        return result;
                    }
                })
                .uid("sup userinfo sup")
                .name("sup userinfo sup");


        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = mapUserInfoDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = mapUserInfoSupDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));

        // base6Line

        /*
        {"birthday":"1979-07-06","decade":1970,"uname":"鲁瑞","gender":"home","zodiac_sign":"巨蟹座","weight":"52","uid":"302","login_name":"9pzhfy3admw3","unit_height":"cm","user_level":"1","phone_num":"13275315996","unit_weight":"kg","email":"9pzhfy3admw3@gmail.com","ts_ms":1747052360573,"age":45,"height":"164"}
        {"birthday":"2005-08-12","decade":2000,"uname":"潘国","gender":"M","zodiac_sign":"狮子座","weight":"68","uid":"522","login_name":"toim614z6zf","unit_height":"cm","user_level":"1","phone_num":"13648187991","unit_weight":"kg","email":"toim614z6zf@hotmail.com","ts_ms":1747052368281,"age":19,"height":"181"}
        {"birthday":"1997-09-06","decade":1990,"uname":"南宫纨","gender":"F","zodiac_sign":"处女座","weight":"53","uid":"167","login_name":"4tjk9p8","unit_height":"cm","user_level":"1","phone_num":"13913669538","unit_weight":"kg","email":"hij36hcc@3721.net","ts_ms":1747052360467,"age":27,"height":"167"}
        */
        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc())
                .uid("process intervalJoin order info")
                .name("process intervalJoin order info");

        // 不分层代码实现 数据量计算太大，查表数据和kafka主题太多，优化数据链路，使用kafka作为中间件进行数据处理
        /*
        SingleOutputStreamOperator<JSONObject> filterOrderInfoAndDetail4BaseModelDs = mapOrderInfoAndDetailModelDs.filter(data -> data.getString("uid") != null && data.isEmpty());
        SingleOutputStreamOperator<JSONObject> filterDevice2BaseModelDs = mapDeviceAndSearchRateResultDs.filter(data -> data.getString("uid") != null && data.isEmpty());

        KeyedStream<JSONObject, String> keyedStreamOrderInfoAndDetail4BaseModelDs = filterOrderInfoAndDetail4BaseModelDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamDevice2BaseModelDs = filterDevice2BaseModelDs.keyBy(data -> data.getString("uid"));*/

        /*
        keyedStreamOrderInfoAndDetail4BaseModelDs.intervalJoin(keyedStreamDevice2BaseModelDs)
                        .between(Time.minutes(-2), Time.minutes(2))
                                .process(new Interval4BaseJoin2BaseFunc()).print();
                                */




        processIntervalJoinUserInfo6BaseMessageDs.map(data -> data.toJSONString())
                        .sinkTo(
                                KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_label_base6_topic)
                        );

        mapOrderInfoAndDetailModelDs.map(data -> data.toJSONString())
                        .sinkTo(
                                KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_label_base4_topic)
                        );

        mapDeviceAndSearchRateResultDs.map(data -> data.toJSONString())
                        .sinkTo(
                                KafkaUtils.buildKafkaSink(kafka_botstrap_servers,kafka_label_base2_topic)
                        );

        processIntervalJoinUserInfo6BaseMessageDs.print("processIntervalJoinUserInfo6BaseMessageDs: ");
        mapDeviceAndSearchRateResultDs.print("mapDeviceAndSearchRateResultDs: ");
        mapOrderInfoAndDetailModelDs.print("mapOrderInfoAndDetailModelDs: ");





        env.execute("DbusUserInfo6BaseLabel");
    }


    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }

    private static String getZodiacSign(LocalDate birthDate) {
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();

        // 星座日期范围定义
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
        else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
        else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
        else if (month == 3 || month == 4 && day <= 19) return "白羊座";
        else if (month == 4 || month == 5 && day <= 20) return "金牛座";
        else if (month == 5 || month == 6 && day <= 21) return "双子座";
        else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
        else if (month == 7 || month == 8 && day <= 22) return "狮子座";
        else if (month == 8 || month == 9 && day <= 22) return "处女座";
        else if (month == 9 || month == 10 && day <= 23) return "天秤座";
        else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
        else return "射手座";
    }
}