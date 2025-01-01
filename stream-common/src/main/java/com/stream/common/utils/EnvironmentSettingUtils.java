package com.stream.common.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 环境参数设置工具类
 * time: 2021/8/25 11:13 className: EnvironmentSettingUtils.java
 *
 * @author han.zhou
 * @version 1.0.0
 */
public final class EnvironmentSettingUtils {

    private static final String HDFS_CHECKPOINT_PATH = ConfigUtils.getString("flink.checkpoint.hdfs.dir");
    private static final String MINIO_CHECKPOINT_PATH = ConfigUtils.getString("flink.checkpoint.minio.dir");

    /**
     * 默认参数设置
     *
     * @param env
     */
    public static void defaultParameter(StreamExecutionEnvironment env) {
        // 开启 checkpoint 支持在 STREAMING 模式下的 FlinkSink 操作
        env.enableCheckpointing(1000 * 30);
        // 设置状态后端为 RocksDB
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        CheckpointConfig config = env.getCheckpointConfig();
        // 设定语义模式，默认情况是 exactly_once
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置 checkpoint 存储路径
        config.setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://cdh01:8020/flink-point/ck"));
        // 设置 checkpoint 超时时间，默认为10分钟
        config.setCheckpointTimeout(10 * 60 * 1000);
        // 设定两个 checkpoint 之间的最小时间间隔，防止出现例如状态数据过大导致 checkpoint 执行时间过长，从而导致 checkpoint 积压过多，
        // 最终 Flink 应用密切触发 checkpoint 操作，会占用大量计算资源而影响整个应用的性能
        config.setMinPauseBetweenCheckpoints(500);
        // 默认情况下，只有一个检查点可以运行
        // 根据用户指定的数量可以同时触发多个 checkpoint，从而提升 checkpoint 整体的效率
        config.setMaxConcurrentCheckpoints(1);
        // 外部检查点
        // 不会在任务正常停止的过程中清理掉检查点数据，而是会一直保存在外部系统介质中，另外也可以通过从外部检查点中对任务恢复 & DELETE_ON_CANCELLATION
        config.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 如果有更近的保存点时，是否作业回退到该检查点
//        config.setPreferCheckpointForRecovery(true);
        // 设置可以允许的 checkpoint 失败数
        config.setTolerableCheckpointFailureNumber(3);

        //** 重启策略相关 *//*
        // 重启3次，每次失败后等待10000毫秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 在5分钟内，只能重启5次，每次失败后最少需要等待10秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));
    }
}
