#!/usr/bin/python
# coding: utf-8
import json
import random
import logging
import sys
import time

from retrying import retry
from typing import Dict
import requests
from datetime import datetime
from multiprocessing import Manager
sys.path.append('..')
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
num_processes = 4
properties = public_func.get_java_properties()

db_config = {
    'host': properties.get("mysql.host"),
    'port': properties.get("mysql.port"),
    'user': properties.get("mysql.user"),
    'password': properties.get("mysql.pwd"),
    'database': properties.get("mysql.spider.db")
}


class SpiderCHNAmapWeatherData:
    def __init__(self, refresh_interval=500, city_result=None, process_id=None):
        self.process_id = process_id
        self.amap_base_url = 'https://restapi.amap.com/v3/weather/weatherInfo?city='
        self.refresh_interval = refresh_interval
        self.last_refresh = datetime.min
        self.sql_query_amap_keys = 'select api_key,key_owner from dev.spider_amap_restapi_key;'
        self.sql_query_chn_city_all = "select code,province,city,area from dev.spider_national_code_compare_dic where city is not null and province <> '澳门特别行政区' and province <> '台湾省' and province <> '香港特别行政区';"
        if city_result is not None:
            self.city_result = city_result
        else:
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
        """封装每个城市数据请求（包含重试）"""
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
            INSERT INTO dev.spider_amap_weather_data_dtl 
            (code, province, city, area, info, lives, report_time)
            VALUES (:code, :province, :city, :area, :info, :lives, :report_time)
                    """
        for index, i in enumerate(self.city_result, 1):
            try:
                current_key = self.get_key()
                resp = self.spider_exec(i.get('code'), current_key.get('api_key'))

                lives_data = resp.get('lives', [{}])[0]
                report_time_str = lives_data.get('reporttime')

                data = {
                    'code': int(i.get('code', 0)),
                    'province': i.get('province', ''),
                    'city': i.get('city', i.get('province', '')),
                    'area': i.get('area', i.get('city', i.get('province', ''))),
                    'info': resp.get('info', ''),
                    'lives': json.dumps(lives_data, ensure_ascii=False),
                    'report_time': report_time_str if report_time_str else None
                }
                data_buffer.append(data)

                if len(data_buffer) >= batch_size:
                    try:
                        public_func.execute_sql(insert_sql, db_config, params=data_buffer, many=True)
                        success_count += len(data_buffer)
                        data_buffer.clear()
                    except Exception as e:
                        data_buffer.clear()

                if index % 100 == 0:
                    self.logger.info(f"[进程 {self.process_id}] 进度: {index}/{len(self.city_result)} 个城市")

            except Exception as e:
                fail_count += 1
                failed_cities.append(str(i.get('code')))
                continue

        if data_buffer:
            try:
                public_func.execute_sql(insert_sql, db_config, params=data_buffer, many=True)
                success_count += len(data_buffer)
            except Exception as e:
                pass

        return {
            "success_count": success_count,
            "fail_count": fail_count,
            "failed_cities": failed_cities,
            "start_time": start_time,
            "end_time": datetime.now()
        }


def run_spider_task(city_chunk, result_dict, process_id):
    """多进程任务执行函数"""
    logger.info(f"进程 {process_id} 启动，处理城市数量: {len(city_chunk)}")
    spider = SpiderCHNAmapWeatherData(city_result=city_chunk, process_id=process_id)
    result = spider.task_spider_data2mysql()
    # 使用共享字典存储结果
    result_dict[process_id] = result
    logger.info(f"进程 {process_id} 完成")


def main():
    # 共享结果字典
    manager = Manager()
    result_dict = manager.dict()

    # 获取全量城市数据
    base_spider = SpiderCHNAmapWeatherData()
    all_cities = base_spider.city_result
    total_cities = len(all_cities)

    # 分割为num个进程处理
    chunk_size = total_cities // num_processes
    chunks = [all_cities[i * chunk_size:(i + 1) * chunk_size] for i in range(num_processes)]

    # 处理余数城市
    remainder = total_cities % num_processes
    if remainder > 0:
        for i in range(remainder):
            chunks[i].append(all_cities[num_processes * chunk_size + i])

    # 构建多进程参数 列表推导式
    func_dict_list = [{
        'func_name': run_spider_task,
        'func_args': (chunk, result_dict, i)  # 传入进程ID
    } for i, chunk in enumerate(chunks)]

    # 调用多进程执行
    public_func.process_thread_func(
        v_func_dict_list=func_dict_list,
        v_type='process',
        v_process_cnt=num_processes
    )

    # 汇总结果
    total_success = 0
    total_fail = 0
    all_failed_cities = []
    time_ranges = []

    for pid, res in result_dict.items():
        total_success += res["success_count"]
        total_fail += res["fail_count"]
        all_failed_cities.extend(res["failed_cities"])
        time_ranges.append((res["start_time"], res["end_time"]))

    # 计算总耗时
    start_times = [t[0] for t in time_ranges]
    end_times = [t[1] for t in time_ranges]
    total_start = min(start_times)
    total_end = max(end_times)
    total_duration = total_end - total_start

    # 推送合并后的消息
    push_msg = {
        "platform": 'Web Spider',
        "context": 'spider_CHN_amap_weather_data.py',
        "total_time": str(total_duration),
        "success_count": total_success,
        "fail_count": total_fail,
        "failed_cities": all_failed_cities[:100]  # 防止消息过长
    }
    public_func.push_feishu_msg(push_msg)
    logger.info("已推送合并统计报告到 Lark")


if __name__ == '__main__':
    main()
