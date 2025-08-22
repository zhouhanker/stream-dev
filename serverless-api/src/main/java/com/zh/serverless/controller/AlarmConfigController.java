package com.zh.serverless.controller;

import com.zh.serverless.service.AlarmConfigService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * @Package com.zh.serverless.controller.AlarmConfigController
 * @Author zhou.han
 * @Date 2025/8/20 20:39
 * @description:
 */

@RestController
@RequestMapping("/api/v2/monitor/warn")
public class AlarmConfigController {

    @Resource
    private AlarmConfigService alarmConfigService;

    @GetMapping("/getAlarmConfig")
    public List<String> getAlarmConfig() {
        return alarmConfigService.getAllAlarmConfigData();
    }
}
