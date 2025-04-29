use realtime_v2_data;
show tables ;

CREATE TABLE IF NOT EXISTS realtime_v2_data.mapping_kf_result_sensitive_words_user(
    user_id bigint,
    msg TEXT,
    consignee varchar(255),
    violation_grade varchar(255),
    violation_msg TEXT,
    is_violation bigint,
    ts_ms bigint,
    ds date comment '分区字段-yyyyMMdd'
)engine=OLAP
    DUPLICATE KEY(user_id)
    PARTITION BY RANGE (ds)()
    DISTRIBUTED BY HASH(`user_id`) BUCKETS 8
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

CREATE ROUTINE LOAD realtime_v2_data.mapping_kf_result_sensitive_words_usero_task
    ON mapping_kf_result_sensitive_words_user
COLUMNS(
    user_id,
    msg,
    consignee,
    violation_grade,
    violation_msg,
    is_violation,
    ts_ms,
    ds
)
PROPERTIES
(
    "format" = "json",
    "strict_mode" = "false",
    "max_batch_rows" = "300000",
    "max_batch_interval" = "30",
    "jsonpaths" = "[
        \"$.user_id\",
        \"$.msg\",
        \"$.consignee\",
        \"$.violation_grade\",
        \"$.violation_msg\",
        \"$.is_violation\",
        \"$.ts_ms\",
        \"$.ds\"
    ]"
)
FROM KAFKA
(
    "kafka_broker_list" = "10.39.48.30:9092,10.39.48.31:9092,10.39.48.32:9092",
    "kafka_topic" = "realtime_v2_result_sensitive_words_user",
    "property.group.id" = "doris_consumer_realtime_v2_result_sensitive_words_user",
    "property.offset" = "earliest",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

select *
from mapping_kf_result_sensitive_words_user;