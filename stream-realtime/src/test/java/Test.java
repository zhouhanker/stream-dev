import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.FlinkEnvUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        StreamExecutionEnvironment env = FlinkEnvUtils.getFlinkRuntimeEnv();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 9999);
        dataStreamSource.print();
        System.err.println(env.getConfig());


        env.execute();
    }
}
