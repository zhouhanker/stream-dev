package com.zh.serverless.bean;
import javax.persistence.*;
import java.time.LocalDateTime;

/**
 * @Package com.zh.serverless.bean.StreamWarnAlarmConfig
 * @Author zhou.han
 * @Date 2025/8/20 20:35
 * @description:
 */

@Entity
@Table(name = "flk_warn_alarm_config", schema = "public")
public class StreamWarnAlarmConfig {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "rule_type")
    private String ruleType;

    @Column(name = "create_user")
    private String createUser;

    @Column(columnDefinition = "jsonb")
    private String data;

    @Column
    private LocalDateTime ts;

    // Getters and Setters
    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getRuleType() { return ruleType; }
    public void setRuleType(String ruleType) { this.ruleType = ruleType; }

    public String getCreateUser() { return createUser; }
    public void setCreateUser(String createUser) { this.createUser = createUser; }

    public String getData() { return data; }
    public void setData(String data) { this.data = data; }

    public LocalDateTime getTs() { return ts; }
    public void setTs(LocalDateTime ts) { this.ts = ts; }
}
