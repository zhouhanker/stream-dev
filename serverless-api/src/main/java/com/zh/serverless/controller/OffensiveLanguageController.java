package com.zh.serverless.controller;

import com.alibaba.fastjson.JSONObject;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @Package com.zh.serverless.controller.OffensiveLanguageController
 * @Author zhou.han
 * @Date 2025/3/15 23:04
 * @description: 冒犯性语言检测
 */

@RestController
@RequestMapping("/api/v2")
public class OffensiveLanguageController {
    String userKey = "23b6_jh_ls_brt_256_tkai_zh_key_fst";

    /*
    curl -X POST \
    http://localhost:8070/api/v2/offensiveLanguage \
            -H "Content-Type: application/json" \
            -d '{
            "key": "test密钥",
            "value": "无需编码的特殊字符 ，我是你爹"}'
    */
    @PostMapping("/offensiveLanguage")
    public ResponseEntity<HashMap<String,Object>> NotCivilized(
            @RequestBody Map<String, String> requestBody){
        HashMap<String, Object> result = new HashMap<>();
        String key = requestBody.get("key");
        if (!key.equals(userKey)){
            result.put("code", 400);
            result.put("msg", "非法请求");
            return ResponseEntity.ok(result);
        }
        String value = requestBody.get("value");
        JSONObject resJsonMsg = new JSONObject();
        resJsonMsg.put("value", value);
        resJsonMsg.put("code", 200);
        resJsonMsg.put("res","脏话");
        result.put("data", resJsonMsg);
        return ResponseEntity.ok(result);
    }
}
