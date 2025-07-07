"""
@Project 
@File    SparkEnvUtils.py
@Author  zhouhan
@Date    2025/6/23 16:02
"""
from pyspark.sql import SparkSession
import logging


class SparkEnv:

	def __init__(self, app_name="spark-task", encoding="UTF-8", configs=None):
		"""
		初始化Spark环境配置

		参数:
			app_name (str): Spark应用名称，默认"readCsv"
			encoding (str): 文件编码，默认"UTF-8"
			configs (dict): 额外的Spark配置项，格式为{k: v}
		"""
		self.app_name = app_name
		self.encoding = encoding
		self.configs = configs or {}
		self.spark = None
		self.sc = None
		self.logger = logging.getLogger(__name__)

	def init_spark_env(self):
		try:
			spark_builder = SparkSession.builder \
				.config("spark.driver.extraJavaOptions", f"-Dfile.encoding={self.encoding}") \
				.appName(self.app_name)
			for k, v in self.configs.items():
				spark_builder = spark_builder.config(k, v)
			spark_builder.enableHiveSupport()
			self.spark = spark_builder.getOrCreate()
			self.sc = self.spark.sparkContext
			self.logger.info(f"Spark环境初始化成功，应用名称: {self.app_name}")
			return self.spark, self.sc

		except Exception as e:
			self.logger.error(f"Spark环境创建失败: {str(e)}")
			raise

	def stop(self):
		if self.spark:
			self.spark.stop()
			self.logger.info("Spark资源已释放")
			self.spark = None
			self.sc = None

	def set_log_level(self, level="WARN"):
		if self.sc:
			self.sc.setLogLevel(level)
			self.logger.info(f"日志级别设置为: {level}")
		else:
			self.logger.warning("请先初始化Spark环境再设置日志级别")

	def __enter__(self):
		self.init_spark_env()
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.stop()
