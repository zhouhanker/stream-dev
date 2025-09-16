#!/usr/bin/python
# coding: utf-8
import logging
import os
from pyspark.sql import SparkSession


import public_func

os.environ["HADOOP_USER_NAME"] = "root"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spider_amap_weather.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("HiveExternalTableExample") \
    .config("hive.metastore.uris", "thrift://cdh02:9083") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("INFO")

target_seatunnel_conf_path = '../../script/seatunnel-conf/batch/bigdata_offline_v1/'
hive_meta_url = 'thrift://cdh02:9083'
hive_hadoop_conf_path = "/etc/hadoop/conf"
hive_warehouse_name = 'bigdata_offline_warehouse_v1'
hive_table_prefix_name = 'ods_mysql8_realtime_v1_'
mysql8_query_sql_prefix = 'select '

properties = public_func.get_java_properties()
db_config = {
    'host': properties.get("mysql.host"),
    'port': properties.get("mysql.port"),
    'user': properties.get("mysql.user"),
    'password': properties.get("mysql.pwd"),
    'database': properties.get("mysql.offline.v1.db")
}


class EtlGenerateMysql8ToHive3SeatunnelConfTask:
    def __init__(self):
        query_mysql_db_sql = 'show tables;'
        res_db_message = public_func.execute_sql(query_mysql_db_sql, db_config, as_dict=True)
        hive_ddl = []
        seatunnel_conf_list = []
        table_names = []
        for i in res_db_message:
            res = public_func.execute_sql(f"desc {i['Tables_in_realtime_v1']} ;", db_config, as_dict=True)
            table_names.append(i['Tables_in_realtime_v1'])
            fields = [item['Field'] for item in res]
            hive_column = ','.join(fields) + ',ds'
            mysql8_all_table_column = ' string ,'.join(fields)
            mysql8_all_column = ', '.join(fields)

            hive_create_table_ddl = f"""
                create external table if not exists {hive_warehouse_name}.{hive_table_prefix_name}{i['Tables_in_realtime_v1']} (
                    {mysql8_all_table_column} string )
                PARTITIONED BY (ds STRING)
                STORED AS PARQUET
                LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/{hive_warehouse_name}/{hive_table_prefix_name}{i['Tables_in_realtime_v1']}/'
                TBLPROPERTIES (
                    'parquet.compress' = 'SNAPPY',
                    'external.table.purge' = 'true'
                );    
            """
            hive_ddl.append(hive_create_table_ddl)

            seatunnel_conf = f"""
            
                env {{
                    parallelism = 2
                    job.mode = "BATCH"
                }}
            
                source{{
                    Jdbc {{
                        url = "jdbc:mysql://{db_config.get("host")}:3306/realtime_v1?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true"
                        driver = "com.mysql.cj.jdbc.Driver"
                        connection_check_timeout_sec = 100
                        user = "root"
                        password = {db_config.get("password")}
                        query = "{mysql8_query_sql_prefix}{mysql8_all_column} ,DATE_FORMAT(NOW(), '%Y%m%d') as ds from {db_config.get("database")}.{i['Tables_in_realtime_v1']}"
                    }}
                }}
            
                transform {{
    
                    }}
                    
                sink {{
                    Hive {{
                        table_name = {hive_warehouse_name}.{hive_table_prefix_name}{i['Tables_in_realtime_v1']}
                        metastore_uri ="{hive_meta_url}"
                        hive.hadoop.conf-path = {hive_hadoop_conf_path}
                        save_mode = "overwrite"
                        partition_by = ["ds"]
                        dynamic_partition = true
                        file_format = "PARQUET"
                        parquet_compress = "SNAPPY"
                        tbl_properties = {{
                            "external.table.purge" = "true"
                        }}
                        fields = [
                            {hive_column}
                        ]
                    }}
                }}
                
            """
            seatunnel_conf_list.append(seatunnel_conf)

        self.hive_ddl = hive_ddl
        self.table_names = table_names
        self.seatunnel_conf_list = seatunnel_conf_list
        self.target_seatunnel_conf_path = target_seatunnel_conf_path

    def generate_hive_tables_func(self):
        self_hive_ddl = self.hive_ddl
        for i in self_hive_ddl:
            logger.info(f"exec create hive ddl: {i}")
            spark.sql(i)
            logger.info(f"{i} is success !")
        pass

    def generate_seatunnel_setting_conf_func(self):
        seatunnel_conf_prefix = 'sync_mysql8_to_hive3_bigdata_offline_ws_ods_'
        if not os.path.exists(target_seatunnel_conf_path):
            os.makedirs(target_seatunnel_conf_path, exist_ok=True)
        for conf_content, table_name in zip(self.seatunnel_conf_list, self.table_names):
            # 构建文件名：前缀 + 表名 + .conf
            file_name = f"{seatunnel_conf_prefix}{table_name}.conf"
            file_path = os.path.join(target_seatunnel_conf_path, file_name)

            # 写入/更新文件
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(conf_content.strip())
            logger.info(f"AT {'update' if os.path.exists(file_path) else 'Create'} Setting Is: {file_path}")

    def exec_seatunnel_script(self):
        abs_target_path = os.path.abspath(self.target_seatunnel_conf_path)
        if not os.path.exists(abs_target_path):
            logger.error(f"Error Target Dir is Not Found ! - {abs_target_path}")
            return

        # 检查是否为目录
        if not os.path.isdir(abs_target_path):
            logger.error(f"Error This Path Is Not Dir - {abs_target_path}")
            return

        conf_files = []
        for filename in os.listdir(abs_target_path):
            # 只处理.conf结尾的文件
            if filename.endswith('.conf'):
                # 构建文件的绝对路径
                file_abs_path = os.path.join(abs_target_path, filename)
                # 确保是文件而不是目录
                if os.path.isfile(file_abs_path):
                    conf_files.append({
                        'name': filename,
                        'absolute_path': file_abs_path
                    })

        res = []
        if conf_files:
            logger.info(f"This {abs_target_path} Have {len(conf_files)} Nums File")
            for file_info in conf_files:
                data = {
                    "task": file_info['name'].split(".")[0]+"_task",
                    "task_path": file_info['absolute_path']
                }
                res.append(data)
        else:
            logger.warning(f"This {abs_target_path} Is Not Have .conf Setting File")

        for i in res:
            public_func.submit_seatunnel_restful_v2_job(i['task_path'], job_name=i['task'])
        return res


if __name__ == '__main__':
    EtlGenerateMysql8ToHive3SeatunnelConfTask().generate_hive_tables_func()
    EtlGenerateMysql8ToHive3SeatunnelConfTask().generate_seatunnel_setting_conf_func()
    EtlGenerateMysql8ToHive3SeatunnelConfTask().exec_seatunnel_script()
