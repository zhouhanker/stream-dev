package com.stream;

import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.crypto.digest.MD5;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.trafficV1.func.SourceAlarmConfig;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.util.MD5Hash;


import java.time.Duration;
import java.util.Date;
import java.util.List;

/**
 * @Package com.stream.Test
 * @Author zhou.han
 * @Date 2024/12/29 22:45
 * @description:
 */
public class Test {

    private static final String KAFKA_BOOTSTRAP_SERVER_LIST = "cdh01:9092,cdh02:9092,cdh03:9092";
    private static final String KAFKA_TOPIC_NAME = "realtime_log_v1";
    private static final String FLINK_UID_VERSION = "_v1";

    @SneakyThrows
    public static void main(String[] args){



        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        MySqlSource<String> mySQLCdcSource = CdcSourceUtils.
                getMySQLCdcSource("test_dev", "", "root", "Zh1028,./", StartupOptions.earliest());


        DataStreamSource<String> mysqlCdcSourceDs = env.fromSource(mySQLCdcSource, WatermarkStrategy.noWatermarks(), "mysql8_cdc");
        mysqlCdcSourceDs.print();


        env.execute();
    }

}
