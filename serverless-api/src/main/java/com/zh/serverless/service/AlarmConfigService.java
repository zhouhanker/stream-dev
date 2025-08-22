package com.zh.serverless.service;

import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Package com.zh.serverless.service.AlarmConfigService
 * @Author zhou.han
 * @Date 2025/8/21 08:46
 * @description:
 */

public interface AlarmConfigService {
    List<String> getAllAlarmConfigData();
}
