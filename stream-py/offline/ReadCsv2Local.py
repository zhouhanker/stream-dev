"""
@Project 
@File    ReadCsv2Local.py
@Author  zhouhan
@Date    2025/6/23 14:00
"""
import datetime

from utils import SparkEnvUtils


def etl_csv_task(path):
	env = SparkEnvUtils.SparkEnv(app_name="read-csv", encoding='UTF-8')
	try:
		spark, sc = env.init_spark_env()
		env.set_log_level("INFO")
		df = spark.read.csv(path, header=True, inferSchema=True)
		print(f"数据行数: {df.count()}")
	finally:
		env.stop()


def main(path):
	etl_csv_task(path)


if __name__ == '__main__':
	local_path = '../data/2024-05-14-detail.csv'
	main(local_path)
