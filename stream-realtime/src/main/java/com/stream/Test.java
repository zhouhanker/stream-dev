package com.stream;

import com.github.houbb.sensitive.word.core.SensitiveWord;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.RedisLuaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.derby.catalog.UUID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @Package com.stream.Test
 * @Author zhou.han
 * @Date 2024/12/29 22:45
 * @description:
 */
public class Test {
    @SneakyThrows
    public static void main(String[] args) {
        String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
        System.err.println(kafka_botstrap_servers);
    }

}
