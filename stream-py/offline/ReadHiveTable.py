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


print(exec_sql)
spark.sql(exec_sql).show()

spark.stop()
