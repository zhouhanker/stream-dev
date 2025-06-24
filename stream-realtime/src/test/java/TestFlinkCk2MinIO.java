import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TestFlinkCk2MinIO {

    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_page_log_topic = ConfigUtils.getString("kafka.page.topic");

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        configureCheckpointing(env);
        configureRestartStrategy(env);

        SingleOutputStreamOperator<String> kafkaPageLogSource = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_page_log_topic,
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_page_log_source"
                ).uid("kafka_page_log_source")
                .name("kafka_page_log_source");


        kafkaPageLogSource.print();





        // 执行作业
        env.execute("Flink Job with MinIO/S3 Checkpointing");
    }

    private static void configureCheckpointing(StreamExecutionEnvironment env) {
        // 配置MinIO/S3作为Checkpoint存储
        String s3Endpoint = "https://074500c63a44cd12e391fe75601335d2.r2.cloudflarestorage.com";
        String s3Bucket = "realtime-v1-flk-point";          // 存储桶名称
        String s3AccessKey = "1f416425ac5798e70403cb6e972d00c9";         // 访问密钥
        String s3SecretKey = "554f7cf864c22b4a2c72d09cdf2da9d769b1a823ba51fd783ee6403f79ff8e33";         // 秘密密钥

        // 配置Hadoop S3A文件系统
        Configuration configuration = new Configuration();
        configuration.setString("fs.s3a.endpoint", s3Endpoint);
        configuration.setString("fs.s3a.access.key", s3AccessKey);
        configuration.setString("fs.s3a.secret.key", s3SecretKey);
        configuration.setBoolean("fs.s3a.path.style.access", true);
        configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
        configuration.setString("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

        // 必须在设置checkpoint存储前配置Hadoop
        org.apache.flink.core.fs.FileSystem.initialize(configuration);

        // S3路径格式: s3a://<bucket>/<path>
        String checkpointPath = "s3a://" + s3Bucket + "/checkpoints/";

        // 启用Checkpoint，每30秒一次
        env.enableCheckpointing(30 * 1000);

        // 设置状态后端为RocksDB
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // 获取Checkpoint配置
        CheckpointConfig config = env.getCheckpointConfig();

        // 设置Checkpoint模式为EXACTLY_ONCE
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置Checkpoint存储路径
        config.setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath));

        // 设置Checkpoint超时时间为10分钟
        config.setCheckpointTimeout(10 * 60 * 1000);

        // 设置两个Checkpoint之间的最小时间间隔为500毫秒
        config.setMinPauseBetweenCheckpoints(500);

        // 设置最大并发Checkpoint数为1
        config.setMaxConcurrentCheckpoints(1);

        // 外部检查点: 取消作业时保留Checkpoint
        config.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置可容忍的Checkpoint失败次数
        config.setTolerableCheckpointFailureNumber(3);
    }

    private static void configureRestartStrategy(StreamExecutionEnvironment env) {
        // 重启策略1: 固定延迟重启，重启3次，每次失败后等待10秒
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));

        // 重启策略2: 失败率重启，在5分钟内最多重启5次，每次失败后最少等待10秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                5,
                Time.of(5, TimeUnit.MINUTES),
                Time.of(10, TimeUnit.SECONDS)));
    }
}