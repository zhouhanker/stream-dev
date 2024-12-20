package com.stream;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.stream.FlinkNcMessage
 * @Author zhou.han
 * @Date 2024/12/17 15:06
 * @description: Test Nc Jar
 */
public class FlinkNcMessage {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> dataStreamSource = env.socketTextStream("cdh03", 14777);

        dataStreamSource.print();

        env.execute();
    }
}
