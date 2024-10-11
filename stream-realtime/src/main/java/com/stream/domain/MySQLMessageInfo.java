package com.stream.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.stream.domain.MySQLMessageInfo
 * @Author zhou.han
 * @Date 2024/10/11 22:04
 * @description: binlog
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class MySQLMessageInfo {
    private String id;
    private String op;
    private String db_name;
    private String log_before;
    private String log_after;
    private String t_name;
    private String ts;
}
