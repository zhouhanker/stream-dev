# !/usr/bin/python
# coding: utf-8
import json
import random
import logging
from retrying import retry
from typing import Dict

import requests
from datetime import datetime

import public_func

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spider_amap_weather.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 重试配置（可调整参数）
RETRY_MAX_ATTEMPTS = 3
RETRY_WAIT_FIXED = 2000
properties = public_func.get_java_properties()

db_config = {
    'host': properties.get("mysql.host"),
    'port': properties.get("mysql.port"),
    'user': properties.get("mysql.user"),
    'password': properties.get("mysql.pwd"),
    'database': properties.get("mysql.spider.db")
}


class SpiderCHNAmapWeatherData:
    def __init__(self, refresh_interval=500):
        self.amap_base_url = 'https://restapi.amap.com/v3/weather/weatherInfo?city='
        self.refresh_interval = refresh_interval
        self.last_refresh = datetime.min
        self.sql_query_amap_keys = 'select api_key,key_owner from dev.spider_amap_restapi_key;'
        self.sql_query_chn_city_all = "select code,province,city,area from dev.spider_national_code_compare_dic where city is not null and province <> '澳门特别行政区' and province <> '台湾省' and province <> '香港特别行政区';"
        self.city_result = public_func.execute_sql(self.sql_query_chn_city_all, db_config, as_dict=True)
        self.logger = logger

    def _refresh_keys(self):
        """刷新密钥列表"""
        self.api_keys = public_func.execute_sql(self.sql_query_amap_keys, db_config, as_dict=True)
        self.last_refresh = datetime.now()
        self.logger.info("密钥列表已刷新")

    def get_key(self):
        """带自动刷新的密钥获取"""
        if (datetime.now() - self.last_refresh).seconds > self.refresh_interval:
            self._refresh_keys()
        return random.choice(self.api_keys)

    @retry(
        stop_max_attempt_number=RETRY_MAX_ATTEMPTS,
        wait_fixed=RETRY_WAIT_FIXED,
        retry_on_exception=lambda e: isinstance(e, (requests.exceptions.RequestException, json.JSONDecodeError)))
    def spider_exec(self, city_code: int, api_key: str) -> Dict:
        """封装单个城市数据请求（包含重试）"""
        req_url = f"{self.amap_base_url}{city_code}&key={api_key}"
        self.logger.debug("尝试请求URL: %s", req_url)

        resp = requests.get(req_url, timeout=5)
        resp.raise_for_status()  # 触发HTTPError异常

        data = resp.json()
        if data.get("status") != "1":
            self.logger.warning("API返回错误: %s", data.get("info", "未知错误"))
            raise requests.exceptions.RequestException(f"API Error: {data.get('info')}")

        return data

    def task_spider_data2mysql(self):
        batch_size = 500
        data_buffer = []
        start_time = datetime.now()
        success_count = 0
        fail_count = 0
        failed_cities = []

        insert_sql = """
            INSERT INTO spider_amap_weather_data_dtl 
            (code, province, city, area, info, lives, report_time)
            VALUES (:code, :province, :city, :area, :info, :lives, :report_time)
                    """
        for index, i in enumerate(self.city_result, 1):
            try:
                # 获取当前使用的API Key（用于日志追踪）
                current_key = self.get_key()
                # self.logger.info("正在处理城市 %s (Key Owner: %s)", i.get('code'), current_key.get('key_owner'))

                # 调用带重试的请求方法
                resp = self.spider_exec(i.get('code'), current_key.get('api_key'))

                lives_data = resp.get('lives', [{}])[0]
                report_time_str = lives_data.get('reporttime')

                # 构建插入数据
                data = {
                    'code': int(i.get('code', 0)),
                    'province': i.get('province', ''),
                    'city': i.get('city', i.get('province', '')),
                    'area': i.get('area', i.get('city', i.get('province', ''))),
                    'info': resp.get('info', ''),
                    'lives': json.dumps(lives_data, ensure_ascii=False),
                    'report_time': report_time_str if report_time_str else None
                }
                self.logger.debug("爬取数据: %s", data)
                data_buffer.append(data)

                # 批量插入逻辑
                if len(data_buffer) >= batch_size:
                    try:
                        public_func.execute_sql(insert_sql, db_config, params=data_buffer, many=True)
                        self.logger.info("已插入 %d 条数据", len(data_buffer))
                        success_count += len(data_buffer)
                        data_buffer.clear()
                    except Exception as e:
                        self.logger.error("插入数据时出错: %s", e, exc_info=True)
                        data_buffer.clear()

                if index % 100 == 0:
                    self.logger.info("进度: %d/%d 个城市", index, len(self.city_result))

            except Exception as e:
                self.logger.error("处理城市 %s 时发生致命错误: %s", i.get('code'), e, exc_info=True)
                fail_count += 1
                failed_cities.append(str(i.get('code')))
                continue

        # 处理剩余数据
        if data_buffer:
            try:
                public_func.execute_sql(insert_sql, db_config, params=data_buffer, many=True)
                self.logger.info("最后插入 %d 条数据", len(data_buffer))
                success_count += len(data_buffer)
            except Exception as e:
                self.logger.error("最后插入数据时出错: %s", e, exc_info=True)
        total_time = datetime.now() - start_time
        self.logger.info("\n[统计报告]")
        self.logger.info("总计耗时: %s", str(total_time))
        self.logger.info("成功插入数据: %d 条", success_count)
        self.logger.info("失败城市数量: %d 个", fail_count)
        if failed_cities:
            self.logger.info("失败城市列表: %s", ", ".join(failed_cities))
        else:
            self.logger.info("失败城市列表: 无")

        push_msg = {
            "platform": 'Web Spider',
            "context": 'spider_CHN_amap_weather_data.py',
            "total_time": str(total_time),
            "success_count": success_count,
            "fail_count": fail_count,
            "failed_cities": failed_cities,
        }
        public_func.push_feishu_msg(push_msg)


def main():
    SpiderCHNAmapWeatherData().task_spider_data2mysql()


if __name__ == '__main__':
    main()

