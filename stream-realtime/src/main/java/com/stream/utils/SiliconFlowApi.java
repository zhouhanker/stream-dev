package com.stream.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.stream.common.utils.ConfigUtils;
import okhttp3.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SiliconFlowApi {


    static {
        JSONObject.DEFAULT_GENERATE_FEATURE |= SerializerFeature.DisableCircularReferenceDetect.getMask();
    }

    private static final ConnectionPool CONNECTION_POOL = new ConnectionPool(200, 5, TimeUnit.MINUTES);
    private static final String SILICON_API_ADDR = "https://api.siliconflow.cn/v1/chat/completions";
    private static final String SILICON_API_TOKEN = ConfigUtils.getString("silicon.api.token");
    private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
            .connectionPool(CONNECTION_POOL)
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build();

    public static String generateBadReview(String prompt, String apiToken) {
        try {
            JSONObject requestBody = buildRequestBody(prompt);
            Thread.sleep(1);
            Request request = new Request.Builder()
                    .url(SILICON_API_ADDR)
                    .post(RequestBody.create(
                            MediaType.parse("application/json; charset=utf-8"),
                            requestBody.toJSONString()
                                        ))
                    .addHeader("Authorization", "Bearer " + apiToken)
                    .addHeader("Content-Type", "application/json")
                    .build();

            try (Response response = CLIENT.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    handleError(response);
                    return "请求失败: HTTP " + response.code();
                }
                return processResponse(response);
            }
        } catch (IOException e) {
            handleException(e);
            return "网络异常: " + e.getMessage();
        } catch (Exception e) {
            return "系统错误: " + e.getMessage();
        }
    }

    private static JSONObject buildRequestBody(String prompt) {
        return new JSONObject()
                .fluentPut("model", "Pro/deepseek-ai/DeepSeek-R1-Distill-Qwen-7B")
                .fluentPut("stream", false)
                .fluentPut("max_tokens", 512)
                .fluentPut("temperature", 0.7)
                .fluentPut("top_p", 0.7)
                .fluentPut("top_k", 50)
                .fluentPut("frequency_penalty", 0.5)
                .fluentPut("n", 1)
                .fluentPut("messages", new JSONObject[]{
                        new JSONObject()
                                .fluentPut("role", "user")
                                .fluentPut("content", prompt)
                });
    }

    private static String processResponse(Response response) throws IOException {
        if (response.body() == null) {
            throw new IOException("空响应体");
        }

        String responseBody = response.body().string();
        JSONObject result = JSON.parseObject(responseBody);

        if (!result.containsKey("choices")) {
            throw new RuntimeException("无效响应结构: 缺少choices字段");
        }

        if (result.getJSONArray("choices").isEmpty()) {
            throw new RuntimeException("响应内容为空");
        }

        return result.getJSONArray("choices")
                .getJSONObject(0)
                .getJSONObject("message")
                .getString("content");
    }

    private static void handleError(Response response) throws IOException {
        String errorBody = response.body() != null ?
                response.body().string() : "无错误详情";
        System.err.println("API错误 [" + response.code() + "]: " + errorBody);
    }

    private static void handleException(IOException e) {
        System.err.println("网络异常: " + e.getMessage());
        e.printStackTrace();
    }

    public static void main(String[] args) {
        // 测试用例（需要有效token）
        String result = generateBadReview(
                "`河南省驻马店市西平县盆尧镇盆尧十字街朱思玉15286869504` 解析上述字符串，解析出姓名，和手机号码，地址，不需要思考过程，直接返回",
                SILICON_API_TOKEN
        );
        System.out.println("生成结果: " + result);
    }
}