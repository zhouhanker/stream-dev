package com.trafficV1.func;

import com.trafficV1.utils.WarnConfigUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.trafficV1.func.SourceAlarmConfig
 * @Author zhou.han
 * @Date 2025/8/21 18:16
 * @description:
 */
public class SourceAlarmConfig implements SourceFunction<List<String>> {

    private static final Logger logger = LoggerFactory.getLogger(SourceAlarmConfig.class);
    private final String configUrl;
    private final long refreshInterval;
    private final TimeUnit timeUnit;
    private volatile boolean isRunning = true;

    public SourceAlarmConfig() {
        this("http://127.0.0.1:8070/api/v2/monitor/warn/getAlarmConfig", 5000, TimeUnit.MILLISECONDS);
    }

    public SourceAlarmConfig(String configUrl, long refreshInterval, TimeUnit timeUnit) {
        this.configUrl = configUrl;
        this.refreshInterval = refreshInterval;
        this.timeUnit = timeUnit;
    }

    @Override
    public void run(SourceContext<List<String>> sourceContext){
        logger.info("开始获取告警配置，URL: {}, 刷新间隔: {} {}", configUrl, refreshInterval, timeUnit.name().toLowerCase());
        while (isRunning){
            try {
                List<String> warnConfig = fetchConfig();
                if (warnConfig != null && !warnConfig.isEmpty()) {
                    sourceContext.collect(warnConfig);
                    logger.info("成功获取{}条告警配置", warnConfig.size());
                    sleep();
                } else {
                    logger.warn("获取到的告警配置为空");
                }
            }catch (Exception e){
                logger.error("获取告警配置时发生异常", e);
                sleep();
            }
        }
        logger.warn("告警配置源已停止");
    }

    @Override
    public void cancel() {
        isRunning = false;
        logger.warn("收到取消信号，将停止获取告警配置");
    }

    private List<String> fetchConfig() {
        return WarnConfigUtils.fetchWarnConfig(configUrl);
    }

    private void sleep() {
        try {
            timeUnit.sleep(refreshInterval);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("休眠被中断", e);
            isRunning = false;
        }
    }

    public String getConfigUrl() {
        return configUrl;
    }

    public long getRefreshInterval() {
        return refreshInterval;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }


}
