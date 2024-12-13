package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.common.utils.PushMessageUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Date;

/**
 * @Package com.retailersv1.DbusMatchDimTable2Hbase
 * @Author zhou.han
 * @Date 2024/12/11 22:38
 * @description: 匹配维度数据到Hbase
 */
public class DbusMatchDimTable2Hbase {

    private static final OutputTag<String> ERR_JSON_MSG = new OutputTag<String>("ERR_JSON_MSG"){};

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> kafkaDbSource = env.fromSource(KafkaUtils.buildKafkaSource(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        ConfigUtils.getString("kafka.topic.db"),
                        new Date().toString(),
                        OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(), "kafka-mysql-db-source");

        SingleOutputStreamOperator<JSONObject> processDs = kafkaDbSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    collector.collect(JSON.parseObject(s));
                } catch (JSONException e) {
                    context.output(ERR_JSON_MSG, s);
                }
            }
        }).uid("process_json_data").name("process_json_data");

        SideOutputDataStream<String> errJsonMsgSideOutput = processDs.getSideOutput(ERR_JSON_MSG);
        errJsonMsgSideOutput.print("Invalid: ").uid("err_json_msg").name("err_json_msg");
        processDs.print("Valid: ").uid("valid_json_msg").name("valid_json_msg");



        env.execute();
    }

}
