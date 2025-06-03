-- create database ddl
CREATE DATABASE IF NOT EXISTS bigdata_insurance_ws
COMMENT '保险业务数据仓库'
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/insurance_ws/'
WITH DBPROPERTIES (
'creator' = 'Eric',
'created_date' = '2025-06-02'
);

-- create external table
CREATE EXTERNAL TABLE IF NOT EXISTS bigdata_insurance_ws.dim_area (
    id bigint,
    province string,
    city string,
    direction string
)
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_insurance_ws/dim_area/'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

-- check
SHOW PARTITIONS bigdata_insurance_ws.dim_area;
-- del table partition
ALTER TABLE bigdata_insurance_ws.dim_area DROP PARTITION (ds = '20250603') PURGE;
-- hdfs dfs -rm -r /bigdata_warehouse/bigdata_insurance_ws/dim_area/ds=20250603/*
