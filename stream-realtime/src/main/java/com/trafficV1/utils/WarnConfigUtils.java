package com.trafficV1.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @Package com.trafficV1.utils.WarnConfigUtils
 * @Author zhou.han
 * @Date 2025/8/21 14:11
 * @description: warning
 */
public class WarnConfigUtils {
    private static final Logger logger = LoggerFactory.getLogger(WarnConfigUtils.class);

    private static final OkHttpClient httpClient = new OkHttpClient();

    private static final String CONFIG_URL = "http://127.0.0.1:8070/api/v2/monitor/warn/getAlarmConfig";


    public static List<String> fetchWarnConfig(String config_url) {

        int maxRetries = 3;
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try (Response response = httpClient.newCall(new Request.Builder()
                    .url(config_url)
                    .get()
                    .build()).execute()) {

                if (response.isSuccessful() && response.body() != null) {
                    String responseBody = response.body().string();
                    JSONArray jsonArray = JSON.parseArray(responseBody);
                    return jsonArray.stream()
                            .map(Objects::toString)
                            .collect(Collectors.toList());
                }

                logger.error("获取预警配置失败，响应码: {}", response.code());

            } catch (IOException e) {
                logger.error("第{}次获取预警配置发生异常", retryCount + 1, e);
            }

            retryCount++;

            if (retryCount < maxRetries) {
                try {
                    TimeUnit.SECONDS.sleep(1 + retryCount);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.warn("重试等待被中断", ie);
                    break;
                }
            }
        }

        logger.error("达到最大重试次数({}次)，获取预警配置失败", maxRetries);
        return null;
    }

    public static void main(String[] args) {
        System.err.println(fetchWarnConfig(CONFIG_URL));
    }
}
