package com.retailersv1;

import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.utils.PaimonMinioUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

/**
 * @Package com.retailersv1.FlkPaimonTest
 * @Author zhou.han
 * @Date 2025/4/6 22:44
 * @description: Flink SQL Paimon Test
 */
public class FlkPaimonTest {

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        PaimonMinioUtils.ExecCreateMinioCatalogAndDatabases(tenv,"minio_paimon_catalog","realtime_v2");

        tenv.executeSql("CREATE TABLE if not exists test.student (  \n" +
                "  `id` int not null,                                   \n" +
                "  `name` varchar(25),                                  \n" +
                "  `age` int,                                           \n" +
                "  `sex` int,                                           \n" +
                "  `ds` string,                                         \n" +
                "  PRIMARY KEY (id,ds) NOT ENFORCED                     \n" +
                ")                                                      \n" +
                "partitioned by(ds)                                     \n" +
                "WITH                                                   \n" +
                "(                                                      \n" +
                "  'deletion-vectors.enabled' = 'true',                 \n" +
                "  'bucket' = '2'                                       \n" +
                ");");

//        tenv.executeSql("insert into test.student values (7, 'y', 15, 1,'20250404');");


        tenv.executeSql("select * from realtime_v2.res_cart_info_tle;").print();

    }
}
