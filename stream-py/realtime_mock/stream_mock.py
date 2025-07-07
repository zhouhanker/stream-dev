import ast
import random
import re
import uuid
from datetime import timedelta, datetime, timezone
from uuid import uuid4

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from typing import Optional, List, Dict, Any, Tuple

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 200)

postgresql_ip = '10.160.60.14'
postgresql_port = 5432
postgresql_user_name = 'postgres'
postgresql_user_pwd = 'Zh1028,./'
postgresql_db = 'spider_db'
postgresql_db_schema = 'public'
postgresql_tbl = 'spider_lululemon_jd_product_dtl'


def get_postgres_data(
        host: str = postgresql_ip,
        port: int = postgresql_port,
        dbname: str = postgresql_db,
        user: str = postgresql_user_name,
        password: str = postgresql_user_pwd,
        query: Optional[str] = None,
        return_df: bool = True
) -> pd.DataFrame | List[Dict[str, Any]]:
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

            if return_df:
                return pd.DataFrame(results)
            else:
                return results

    except Exception as e:
        print(f"查询数据库时发生错误: {e}")
        raise
    finally:
        if conn is not None:
            conn.close()


def generate_uuid_order_id():
    return uuid.uuid4().hex




def random_choice(lst):
    if len(lst) == 0:
        return None
    return random.choice(lst)


def generate_random_timestamp(start_date: str, end_date: str, timezone_offset: int = 8) -> tuple[int, str]:
    tz = timezone(timedelta(hours=timezone_offset))

    start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=tz)
    end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=tz)

    current_dt = datetime.now(tz)

    end_timestamp = min(end_dt.timestamp(), current_dt.timestamp())

    start_timestamp = start_dt.timestamp()
    if start_timestamp > end_timestamp:
        raise ValueError("开始时间不能晚于结束时间或当前时间")

    tz_cn = timezone(timedelta(hours=8))
    ts_ms = int(random.uniform(start_timestamp, end_timestamp))
    ds_ms = datetime.fromtimestamp(ts_ms, tz=tz_cn).strftime('%Y-%m-%d %H:%M:%S')
    return ts_ms, ds_ms


if __name__ == '__main__':
    # noinspection SqlResolve
    query_sql = """
        select id, product_link, sale_amount, product_desc, product_id, item_id, material, is_online_sales, color_list, size_list, comments, recommendations_product_id, ts
        from spider_db.public.spider_lululemon_jd_product_dtl
        where product_id is not null;
    """
    ts, ds = generate_random_timestamp("2025-07-01 00:00:00", "2025-07-01 23:59:59")
    lst = get_postgres_data(query=query_sql, return_df=False)

    for i in lst:
        print(i)
