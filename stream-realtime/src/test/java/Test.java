import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.FlinkEnvUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcFileFormatFactory;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Date;

/**
 * @Package PACKAGE_NAME.Test
 * @Author zhou.han
 * @Date 2024/12/17 15:04
 * @description:
 */
public class Test {


    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                "realtime_v3",
                "realtime_v3.cdc_test_tbl",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );


        DataStreamSource<String> ds = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "test-cdc-source");
        ds.print();


        env.execute();
    }



}
