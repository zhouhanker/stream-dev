import requests
import json
import logging
import threading
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

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

db_params = {
    "host": "10.160.60.14",
    "port": "5432",
    "database": "spider_db",
    "user": "postgres",
    "password": "Zh1028,./"
}

# API URL
CMB_EXCHANGE_RATE_URL = 'https://fx.cmbchina.com/api/v1/fx/rate'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
}


def fetch_data_2postgresql():
    resp = requests.get(CMB_EXCHANGE_RATE_URL)
    resp_json = resp.json()
    res = []
    for i in resp_json['body']:
        ccy_zh = i['ccyNbr']
        ccy_en = i['ccyNbrEng'].split(' ')[1]
        date_part = i['ratDat'].replace('年', '-').replace('月', '-').replace('日', '')
        time_part = i['ratTim']
        ct_time = f"{date_part} {time_part}"
        data = {
            "ccy_zh": i['ccyNbr'],
            "ccy_en": i['ccyNbrEng'].split(' ')[1],
            "rtbBid": i['rtbBid'],
            "rthOfr": i['rthOfr'],
            "rtcOfr": i['rtcOfr'],
            "rthBid": i['rthBid'],
            "rtcBid": i['rtcBid'],
            "ratTim": i['ratTim'],
            "ratDat": i['ratDat']
        }
        res.append((ccy_zh, ccy_en, ct_time, json.dumps(data)))
    if not res:
        logger.info("No valid data to insert")
        return 0
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cursor:
            # SQL插入语句
            insert_query = sql.SQL("""
                    INSERT INTO public.spider_exchange_rate_dtl (ccy_zh, ccy_en,ct_time, data)
                    VALUES (%s, %s, %s, %s)
                """)

            # 批量插入数据
            execute_batch(cursor, insert_query, res)
            conn.commit()

    logger.info(f"成功插入 {len(res)} 条记录")


def main():
    fetch_data_2postgresql()
    threading.Timer(10, main).start()


if __name__ == '__main__':
    main()
