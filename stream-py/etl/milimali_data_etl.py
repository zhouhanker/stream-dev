import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
import os


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

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 200)


# 获取数据库配置
def get_db_config():
    return {
        'host': os.getenv('DB_HOST', '10.160.60.17'),
        'port': os.getenv('DB_PORT', '3306'),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', 'Zh1028,./'),
        'database': os.getenv('DB_NAME', 'mili_mali_dev'),
        'charset': 'utf8mb4'
    }


# 创建数据库引擎
def create_db_engine(db_config):
    connection_string = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        f"?charset={db_config['charset']}"
    )
    return create_engine(connection_string)


# 列映射配置
def get_column_mapping():
    return {
        '达人名称': 'daren_name',
        '达人号': 'daren_num',
        '跟进媒介': 'medium',
        '视频号ID': 'video_id',
        '合作阶段': 'cooperate_stage',
        'UID': 'uid',
        '近30日成交金额': 'sale_amount_30d_sum',
        '成交金额': 'sale_amount',
        '支付销量': 'pay_sale_num',
        '成交商品': 'deal_product',
        '绑定时间': 'binding_time',
        '近7天成交金额环比': 'pay_7d_sale_amount_qoq',
        '微信': 'wechat_id',
        '手机号': 'phone_number',
        '邮箱': 'email',
        'QQ': 'qq_id',
        '其他联系方式': 'other_contact',
    }


# 读取和预处理数据
def load_and_preprocess_data(file_path, column_mapping, platform):
    try:
        df = pd.read_csv(file_path).rename(columns=column_mapping)
        df['plat_from'] = platform

        # 清理特殊字符
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace('[\n\t]', '', regex=True)

        return df
    except FileNotFoundError:
        logger.error(f"文件未找到: {file_path}")
        raise
    except Exception as e:
        logger.error(f"数据处理出错: {e}")
        raise


# 写入数据库
def write_to_database(df, engine, table_name):
    try:
        with engine.connect() as conn:
            # 测试数据库连接
            conn.execute(text("SELECT 1"))
            logger.info("数据库连接成功")

            # 写入数据
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                chunksize=1000
            )
            logger.info(f"成功写入 {len(df)} 条数据到表 {table_name}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"数据库操作出错: {e}")
        return False


# 主流程
def main(tabel_name, data_path, plat_from):
    try:
        # 初始化配置
        db_config = get_db_config()
        engine = create_db_engine(db_config)
        column_mapping = get_column_mapping()

        # 处理数据
        data_frame = load_and_preprocess_data(data_path, column_mapping, plat_from)
        logger.info(f"数据加载完成，共 {len(data_frame)} 条记录")

        # 写入数据库
        success = write_to_database(data_frame, engine, tabel_name)

        if success:
            logger.info("ETL 流程成功完成")
        else:
            logger.error("ETL 流程失败")

        return 0 if success else 1

    except Exception as e:
        logger.exception(f"ETL 流程异常终止: {e}")
        return 1


if __name__ == "__main__":

    daren_medium_info_table_name = 'daren_medium_info'
    douyin_csv_path = '/Users/zhouhan/dev_env/data/mila_mali/newdydetail.csv'
    ks_csv_path = '/Users/zhouhan/dev_env/data/mila_mali/newksdetail.csv'
    # sph_csv_path = '/Users/zhouhan/dev_env/data/mila_mali/new.csv'
    xhs_csv_path = '/Users/zhouhan/dev_env/data/mila_mali/newxhsdetail.csv'

    main(tabel_name=daren_medium_info_table_name, data_path=douyin_csv_path, plat_from='douyin')
    main(tabel_name=daren_medium_info_table_name, data_path=ks_csv_path, plat_from='ks')
    # main(tabel_name=daren_medium_info_table_name, data_path=sph_csv_path, plat_from='sph')
    main(tabel_name=daren_medium_info_table_name, data_path=xhs_csv_path, plat_from='xhs')

