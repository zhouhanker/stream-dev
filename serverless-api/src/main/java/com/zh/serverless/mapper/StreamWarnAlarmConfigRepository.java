package com.zh.serverless.mapper;

import com.zh.serverless.bean.StreamWarnAlarmConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * @Package com.zh.serverless.mapper.StreamWarnAlarmConfigRepository
 * @Author zhou.han
 * @Date 2025/8/21 08:38
 * @description:
 */
@Repository
public interface StreamWarnAlarmConfigRepository extends JpaRepository<StreamWarnAlarmConfig, Integer> {
}
