package com.retailersv1.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.retailersv1.domain.TableProcessDim
 * @Author zhou.han
 * @Date 2024/12/20 09:02
 * @description: /
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim implements Serializable {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 hbase 的列族
    String sinkFamily;

    String sinkRowKey;

    String op;
}
