#!/usr/bin/python
# coding: utf-8

import logging
import public_func

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

target_seatunnel_conf_path = '../../script/seatunnel-conf/batch/bigdata_offline_v1/'
hive_warehouse_name = 'bigdata_offline_warehouse_v1'
hive_table_prefix_name = 'ods_mysql8_realtime_v1_'

properties = public_func.get_java_properties()
db_config = {
    'host': properties.get("mysql.host"),
    'port': properties.get("mysql.port"),
    'user': properties.get("mysql.user"),
    'password': properties.get("mysql.pwd"),
    'database': properties.get("mysql.offline.v1.db")
}
logger.info(f"db setting is {db_config}")


class ETLGenerateHiveDDL:
    def __init__(self):
        query_mysql8_all_tables_sql = "show tables;"
        tables_name_lists = public_func.execute_sql(query_mysql8_all_tables_sql, db_config, as_dict=True)
        for i in tables_name_lists:
            table_lists = public_func.execute_sql(f"desc {i['Tables_in_realtime_v1']};", db_config, as_dict=True)
            fields = [item['Field'] for item in table_lists]
            columns = ' string, '.join(fields)
            hive_create_ddl = f"""
                create external table if not exists {hive_warehouse_name}.{i['Tables_in_realtime_v1']} (
                    {columns} string
                )
                PARTITIONED BY (ds STRING)
                STORED AS PARQUET
                LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/{hive_warehouse_name}/{i['Tables_in_realtime_v1']}/'
                TBLPROPERTIES (
                    'parquet.compress' = 'SNAPPY',
                    'external.table.purge' = 'true'
                )
            """
            print(hive_create_ddl)
        pass


if __name__ == '__main__':
    ETLGenerateHiveDDL()

