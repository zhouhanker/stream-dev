package com.stream.common.utils;

import com.alibaba.fastjson.JSONObject;
import okhttp3.*;

/**
 * @Package com.stream.common.utils.PushMessageUtils
 * @Author zhou.han
 * @Date 2024/12/11 23:26
 * @description: Push Message
 */
public class PushMessageUtils {

    public static void PushFeishuMsg(String messageType,String platformUrl,String platform,String context){
        JSONObject msg = new JSONObject();
        msg.put("message_type", messageType);
        msg.put("platform_url", platformUrl);
        msg.put("platform", platform);
        msg.put("context", context);
        OkHttpClient client = new OkHttpClient();
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), msg.toString());
        Request request = new Request.Builder()
                .url(ConfigUtils.getString("push.feishu.url"))
                .post(requestBody)
                .build();
        try {
            Response response = client.newCall(request).execute();
            System.out.println("Response status code: " + response.code());
            if (response.body() != null) {
                System.out.println("Response content: " + response.body().string());
                response.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            client.dispatcher().executorService().shutdown();
            client.connectionPool().evictAll();
        }
    }

    public static void main(String[] args) {
        PushFeishuMsg("err","http://application_98","flink","{jsonsss}");
    }
}
