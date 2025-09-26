package com.v3;

import com.stream.common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.v3.SocketStreamTest
 * @Author zhou.han
 * @Date 2025/9/24 19:26
 * @description:
 */
public class SocketStreamTest {

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("cdh01", 10265);

        dataStreamSource.print();


        env.execute();
    }

}
