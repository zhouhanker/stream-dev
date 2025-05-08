#!/usr/bin/python
# coding: utf-8
import logging

import pandas as pd
from sqlalchemy import create_engine, BigInteger, String

DB_CONFIG = {
    'host': '10.160.60.17',
    'port': 3306,
    'user': 'root',
    'password': 'Zh1028,./',
    'database': 'dev'
}

engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset=utf8mb4"
)

file_df = pd.read_csv('../resource/national-code.txt', sep=' ', header=None, engine='python', names=['code', 'origin'])
split_cols = file_df['origin'].str.split(',', n=2, expand=True)
split_cols.columns = ['province', 'city', 'area']
file_df = pd.concat([file_df, split_cols], axis=1).drop(columns=['origin'])
file_df = file_df.where(pd.notnull(file_df), None)
file_df['code'] = file_df['code'].astype('int64')
final_df = file_df[['code', 'province', 'city', 'area']]

try:
    final_df.to_sql(
        name='spider_national_code_compare_dic',
        con=engine,
        if_exists='append',
        index=False,
        dtype={
            'code': BigInteger,
            'province': String(50),
            'city': String(50),
            'area': String(50)
        }
    )
    inserted_count = len(final_df)
    print(f"数据表新建成功，插入了 {inserted_count} 条数据！")
except Exception as e:
    print(f"插入失败，错误信息：{e}")
