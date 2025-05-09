create database if not exists realtime_v2_data;
use realtime_v2_data;
show tables ;

SHOW ROUTINE LOAD;
-- 恢复
RESUME ROUTINE LOAD FOR mapping_kf_comment_info_task;
-- 停止，需要重新创建
stop ROUTINE LOAD FOR mapping_kf_comment_info_task;
-- 查看
SHOW ROUTINE LOAD WHERE Name = "mapping_kf_comment_info_task";


CREATE TABLE IF NOT EXISTS realtime_v2_data.mapping_kf_comment_info
(
    `id` BIGINT,
    `info_original_total_amount` VARCHAR(255),
    `info_activity_reduce_amount` VARCHAR(255),
    `commentTxt` TEXT,
    `info_province_id` INT,
    `info_payment_way` VARCHAR(50),
    `ds` DATE COMMENT '分区字段',
    `info_create_time` BIGINT,
    `info_refundable_time` BIGINT,
    `info_order_status` VARCHAR(50),
    `spu_id` BIGINT,
    `table` VARCHAR(50),
    `info_tm_ms` BIGINT,
    `info_operate_time` BIGINT,
    `op` VARCHAR(10),
    `create_time` BIGINT,
    `info_user_id` BIGINT,
    `info_op` VARCHAR(10),
    `info_trade_body` TEXT,
    `sku_id` BIGINT,
    `server_id` VARCHAR(10),
    `dic_name` VARCHAR(50),
    `info_consignee_tel` VARCHAR(20),
    `info_total_amount` VARCHAR(50),
    `info_out_trade_no` VARCHAR(50),
    `appraise` VARCHAR(50),
    `user_id` BIGINT,
    `info_id` BIGINT,
    `info_coupon_reduce_amount` VARCHAR(50),
    `order_id` BIGINT,
    `info_consignee` VARCHAR(255),
    `ts_ms` BIGINT,
    `db` VARCHAR(50)
)
    engine=OLAP
    DUPLICATE KEY(`id`)
    PARTITION BY RANGE (ds)()
    DISTRIBUTED BY HASH(`id`) BUCKETS 8
    PROPERTIES (
                   "dynamic_partition.enable" = "true",
                   "dynamic_partition.time_unit" = "DAY",
                   "dynamic_partition.start" = "-90",
                   "dynamic_partition.end" = "30",
                   "dynamic_partition.prefix" = "p",
                   "dynamic_partition.buckets" = "8",
                   "dynamic_partition.create_history_partition" = "true",
                   "replication_num" = "1"
               );


-- create Routing load task
CREATE ROUTINE LOAD realtime_v2_data.mapping_kf_comment_info_task ON mapping_kf_comment_info
COLUMNS(
    info_original_total_amount,
    info_activity_reduce_amount,
    commentTxt,
    info_province_id,
    info_payment_way,
    ds,
    info_create_time,
    info_refundable_time,
    info_order_status,
    id,
    spu_id,
    `table`,
    info_tm_ms,
    info_operate_time,
    op,
    create_time,
    info_user_id,
    info_op,
    info_trade_body,
    sku_id,
    server_id,
    dic_name,
    info_consignee_tel,
    info_total_amount,
    info_out_trade_no,
    appraise,
    user_id,
    info_id,
    info_coupon_reduce_amount,
    order_id,
    info_consignee,
    ts_ms,
    db
)
PROPERTIES
(
    "format" = "json",
    "strict_mode" = "false",
    "max_batch_rows" = "300000",
    "max_batch_interval" = "30",
    "jsonpaths" = "[
        \"$.info_original_total_amount\",
        \"$.info_activity_reduce_amount\",
        \"$.commentTxt\",
        \"$.info_province_id\",
        \"$.info_payment_way\",
        \"$.ds\",
        \"$.info_create_time\",
        \"$.info_refundable_time\",
        \"$.info_order_status\",
        \"$.id\",
        \"$.spu_id\",
        \"$.table\",
        \"$.info_tm_ms\",
        \"$.info_operate_time\",
        \"$.op\",
        \"$.create_time\",
        \"$.info_user_id\",
        \"$.info_op\",
        \"$.info_trade_body\",
        \"$.sku_id\",
        \"$.server_id\",
        \"$.dic_name\",
        \"$.info_consignee_tel\",
        \"$.info_total_amount\",
        \"$.info_out_trade_no\",
        \"$.appraise\",
        \"$.user_id\",
        \"$.info_id\",
        \"$.info_coupon_reduce_amount\",
        \"$.order_id\",
        \"$.info_consignee\",
        \"$.ts_ms\",
        \"$.db\"
    ]"
)
FROM KAFKA
(
    "kafka_broker_list" = "cdh01:9092,cdh01:9092,cdh01:9092",
    "kafka_topic" = "realtime_v2_fact_comment_db",
    "property.group.id" = "doris_consumer_group",
    "property.offset" = "earliest",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);


select *
from mapping_kf_comment_info;