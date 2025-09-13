package com.trafficV1;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.common.utils.WaterMarkUtils;
import com.trafficV1.func.SourceAlarmConfig;
import com.trafficV1.func.WarnKeyedBroadCastProcessFunc;
import lombok.SneakyThrows;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.Date;
import java.util.List;

/**
 * @Package com.trafficV1.DbusWarnVehicleAlarmAPP
 * @Author zhou.han
 * @Date 2025/8/19 11:01
 * @description:
 */
public class DbusWarnVehicleAlarmAPP {

    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_topic_traffic_car_info = "realtime_v3_traffic_origin_data_info";
    private static final OutputTag<String> Emergency_Alarm_Tag = new OutputTag<String>("Emergency_Alarm_Tag"){};
    private static final OutputTag<String> Vehicleno_Alarm_Tag = new OutputTag<String>("Vehicleno_Alarm_Tag"){};

    private static final String FLINK_UID_VERSION = "_v1";

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 禁用Flink 的泛型推测
//        env.getConfig().disableGenericTypes();
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> sourceKafkaDs = env.fromSource(
                KafkaUtils.buildKafkaSource(kafka_botstrap_servers, kafka_topic_traffic_car_info, new Date() + "_traffic_origin_data_info", OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts",3),
                "kafka_source_topic_traffic_car_info" + FLINK_UID_VERSION
        );

        SingleOutputStreamOperator<List<String>> sourceWarnConfigDs = env.addSource(new SourceAlarmConfig(), TypeInformation.of(new TypeHint<List<String>>() {
                }))
                .setParallelism(1)
                .uid("source_warn_api_config" + FLINK_UID_VERSION)
                .name("source_warn_api_config");

        SingleOutputStreamOperator<JSONObject> convert2JsonDs = sourceKafkaDs.map(JSON::parseObject)
                .returns(Types.GENERIC(JSONObject.class))
                .uid("convert2JsonDs")
                .name("convert2JsonDs");

        KeyedStream<JSONObject, String> KeyedVehiclenoDs = convert2JsonDs.keyBy(data -> data.getString("vehicleno"));

        MapStateDescriptor<Void, List<String>> warnConfigMapStateDesc = new MapStateDescriptor<>("warnConfigMapStateDesc", Types.VOID, Types.LIST(Types.STRING));
        BroadcastStream<List<String>> broadcastWarnConfig = sourceWarnConfigDs.broadcast(warnConfigMapStateDesc);

        BroadcastConnectedStream<JSONObject, List<String>> connectMainDsAndConfigDs = KeyedVehiclenoDs.connect(broadcastWarnConfig);
        SingleOutputStreamOperator<JSONObject> processWarnBcDs = connectMainDsAndConfigDs.process(new WarnKeyedBroadCastProcessFunc(Emergency_Alarm_Tag, Vehicleno_Alarm_Tag, warnConfigMapStateDesc))
                .uid("process_warn_rule_broadcast_ds" + FLINK_UID_VERSION)
                .name("process_warn_rule_broadcast_ds");

        SideOutputDataStream<String> warnEmAlarmSideOutputDs = processWarnBcDs.getSideOutput(Emergency_Alarm_Tag);
        SideOutputDataStream<String> warnVeAlarmSideOutputDs = processWarnBcDs.getSideOutput(Vehicleno_Alarm_Tag);

        warnEmAlarmSideOutputDs.print("warnEmAlarmSideOutputDs -> ");
        warnVeAlarmSideOutputDs.print("warnVeAlarmSideOutputDs -> ");


        env.execute();
    }
}
