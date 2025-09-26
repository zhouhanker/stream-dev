"""
@Project
@File    ReadCsv2Local.py
@Author  zhouhan
@Date    2025/6/23 14:00
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveExternalTableExample") \
    .config("hive.metastore.uris", "thrift://cdh02:9083") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("INFO")

# env = SparkEnvUtils.SparkEnv(app_name='test-read-hive-data-task', encoding='UTF-8')
# spark_env, sc = env.init_spark_env()

exec_sql = """
    select /*+ broadcast(ods_base_category1) */
           c1.id,
           c1.name,
           c2.name as c2_name
    from bigdata_offline_v1_ws.ods_base_category2 as c2
    join bigdata_offline_v1_ws.ods_base_category1 as c1
    on c1.id = c2.category1_id;
"""

sql = """
    select *
    from (
        select id,
               consignee,
               consignee_tel,
               reflect("org.apache.commons.lang.math.NumberUtils","isNumber",consignee_tel) as flag
        from bigdata_offline_warehouse_v1.ods_mysql8_realtime_v1_order_info
         ) as t
    where flag = "false";
"""

df_exec_setting_tbl = spark.sql(sql)
df_exec_setting_tbl.show()

spark.stop()
