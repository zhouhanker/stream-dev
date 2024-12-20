package com.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

/**
 * @Package com.stream.FlinkSinktoDorisTest
 * @Author zhou.han
 * @Date 2024/12/16 13:44
 * @description: Test
 */
public class FlinkSink2DorisTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> dataStreamSource = env.fromSource(
                KafkaUtils.buildKafkaSource("cdh01:9092,cdh02:9092,cdh03:9092", "zh_test", new Date().toString(), OffsetsInitializer.latest()),
                WatermarkStrategy.noWatermarks(),
                "test-kafka"
        );

        dataStreamSource.print();

        dataStreamSource.sinkTo(sink2DorisFunc("dev_t_zh.dws_trade_cart_add_uu_window")).setParallelism(1);

        env.execute();
    }


    public static DorisSink<String> sink2DorisFunc(String tableName){
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes("10.39.48.33:8030")
                                .setTableIdentifier(tableName)
                                .setUsername("admin")
                                .setPassword("zh1028,./")
                                .build()
                )
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        .setLabelPrefix("doris_label_"+new Date().getTime())  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(4) // 用于缓存stream load数据的缓冲条数: 默认 3
                        .setBufferSize(1024*1024) //用于缓存stream load数据的缓冲区大小: 默认 1M
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}
