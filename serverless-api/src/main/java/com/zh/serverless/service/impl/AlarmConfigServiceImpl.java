package com.zh.serverless.service.impl;

import com.zh.serverless.bean.StreamWarnAlarmConfig;
import com.zh.serverless.mapper.StreamWarnAlarmConfigRepository;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Package com.zh.serverless.service.impl.AlarmConfigServiceImpl
 * @Author zhou.han
 * @Date 2025/8/21 08:47
 * @description:
 */
@Service
public class AlarmConfigServiceImpl implements com.zh.serverless.service.AlarmConfigService {

    @Resource
    private StreamWarnAlarmConfigRepository streamWarnAlarmConfigRepository;

    @Override
    public List<String> getAllAlarmConfigData() {
        List<StreamWarnAlarmConfig> configMessage = streamWarnAlarmConfigRepository.findAll();
        return configMessage.stream()
                .map(StreamWarnAlarmConfig::getData)
                .collect(Collectors.toList());
    }
}
