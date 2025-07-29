import pandas as pd
from sqlalchemy import create_engine

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 200)

# FIXME Please you mysql connection message
table_name = ''
mysql_ip = ''
mysql_port = ""
mysql_user_name = ''
mysql_user_pwd = ''
mysql_db = ''
# FIXME you csv file path
csv_file_path = ''
db_config = {
    'host': mysql_ip,
    'port': mysql_port,
    'user': mysql_user_name,
    'password': mysql_user_pwd,
    'database': mysql_db,
    'charset': 'utf8mb4'
}


def replace_newline(column):
    return column.fillna('').astype(str).str.replace('\n', '&', regex=True)


pd_read_csv = pd.read_csv(csv_file_path)

# 需要处理的列名列表
columns_to_process = ['最新跟进记录', '地址信息']

# 处理换行符
pd_read_csv[columns_to_process] = pd_read_csv[columns_to_process].apply(replace_newline)

# 列名映射字典：CSV列名 -> 数据库列名
column_mapping = {
    '达人名称': 'daren_name',
    '达人号': 'daren_num',
    '跟进媒介': 'followup_medium',
    '合作阶段': 'cooperate_stage',
    '跟进记录数': 'followup_cnt',
    '地址信息': 'address_message',
    'UID': 'uid',
    '最新跟进记录': 'last_followup_message',
    '带货水平': 'pay_level',
    '近30日成交金额': 'pay_amount_30d',
    '成交金额': 'pay_amount',
    '支付销量': 'pay_sale_count',
    '成交商品': 'pay_product_num',
    '近7天成交金额环比': 'pay_sale_amount_7da_QOQ',
    '微信': 'wechat_id',
    '手机号': 'phone',
    '邮箱': 'email',
    'QQ': 'qq_id',
    '其他联系方式': 'other_link_type'
}

# 重命名DataFrame的列
pd_read_csv = pd_read_csv.rename(columns=column_mapping)

# 确保只包含数据库表中存在的列
db_columns = list(column_mapping.values())
pd_read_csv = pd_read_csv[db_columns]

# 创建数据库连接引擎
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config['charset']}")

try:
    # 将DataFrame写入MySQL数据库
    pd_read_csv.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False,
        chunksize=1000
    )
    print(f"成功将 {len(pd_read_csv)} 条记录写入 {table_name} 表")
except Exception as e:
    print(f"写入数据库时发生错误: {e}")
finally:
    engine.dispose()
