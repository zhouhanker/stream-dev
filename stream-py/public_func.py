import json
import threading
from contextlib import contextmanager
from multiprocessing import Pool
from typing import Dict, Optional, Tuple, Union, List

import javaproperties
import urllib3
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import requests
import os
import logging
urllib3.disable_warnings()


def setup_logger():
    logger = logging.getLogger('SeatunnelJobSubmitter')
    logger.setLevel(logging.INFO)

    # 控制台日志格式
    console_handler = logging.StreamHandler()
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(console_format)

    file_handler = logging.FileHandler('seatunnel_job_submitter.log', mode='w')
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_format)
    file_handler.setLevel(logging.DEBUG)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


log = setup_logger()


@contextmanager
def get_db_connection(connection_str: str):
    """数据库连接上下文管理器"""
    engine = create_engine(connection_str)
    conn = None
    try:
        conn = engine.connect()
        yield conn
    except SQLAlchemyError as e:
        print(f"数据库连接失败: {e}")
        raise
    finally:
        if conn:
            conn.close()
        engine.dispose()


def execute_sql(
        sql: str,
        connection_params: Dict[str, Union[str, int]],
        params: Optional[Union[Dict, Tuple, List[Dict]]] = None,
        as_dict: bool = False,
        many: bool = False
) -> Optional[Union[List[Dict], List[tuple], int]]:
    """
    增强版SQL执行方法（支持批量操作）

    :param sql: SQL语句
    :param connection_params: 数据库连接配置
    :param params: 单条参数（Dict/Tuple）或批量参数（List[Dict]）
    :param as_dict: 是否返回字典格式结果
    :param many: 是否批量操作
    :return:
        - 查询操作：结果列表
        - 写入操作：影响行数
        - 错误时返回None
    """
    try:
        # 构建连接字符串
        driver = connection_params.get("driver", "pymysql")
        connection_str = (
            f"mysql+{driver}://{connection_params['user']}:"
            f"{connection_params['password']}@"
            f"{connection_params['host']}:"
            f"{connection_params.get('port', 3306)}/"
            f"{connection_params['database']}?charset=utf8mb4"
        )

        with get_db_connection(connection_str) as conn:
            # 判断操作类型
            is_write_operation = any(
                sql.strip().lower().startswith(cmd)
                for cmd in ['insert', 'update', 'delete']
            )

            # 执行SQL
            if many:  # 批量操作模式
                if not isinstance(params, list):
                    raise ValueError("批量操作需要List类型参数")

                result = conn.execute(text(sql), params)
                affected_rows = result.rowcount if result.rowcount != -1 else len(params)
            else:  # 单条操作模式
                result = conn.execute(text(sql), params)
                affected_rows = result.rowcount

            # 提交事务
            if is_write_operation:
                conn.commit()  # 添加事务提交

            # 处理结果
            if result.returns_rows:
                rows = result.fetchall()
                return [dict(row._asdict()) if as_dict else tuple(row) for row in rows]
            else:
                return affected_rows if is_write_operation else None

    except SQLAlchemyError as e:
        print(f"[SQL执行错误] {str(e)}")
        return None
    except ValueError as e:
        print(f"[参数错误] {str(e)}")
        return None


def process_thread_func(v_func_dict_list, v_type, v_process_cnt=1, v_thread_max=21):
    """
        批量线程执行方法
        :param v_func_dict_list:
        :param v_thread_max:
        :param v_func_list_dict 传入方法list、参数集合 exp:[{'func_name':'arg1_name,arg2_name'},{'func_name1':'arg1_name,arg2_name'}]
        :param v_type 线程 thread 进程 process
        :param v_process_cnt 进程数 v_thread_max 线程数
    """
    thread_list = []
    # 设置最大线程数
    thread_max = v_thread_max
    if v_func_dict_list and v_type == 'thread':
        for i in v_func_dict_list:
            func_name = i['func_name']
            func_args = i['func_args']
            t = threading.Thread(target=func_name, name=i, args=func_args)
            t.start()
            thread_list.append(t)
            print('线程方法 {} 已运行'.format(func_name.__name__))
            if threading.active_count() == thread_max:
                for j in thread_list:
                    j.join()
                thread_list = []
        for j in thread_list:
            j.join()
        print('所有线程方法执行结束')

    elif v_func_dict_list and v_type == 'process':
        pool = Pool(processes=v_process_cnt)
        for i in v_func_dict_list:
            try:
                func_name = i['func_name']
                func_args = i['func_args']
                pool.apply_async(func=func_name, args=func_args)
                print('进程方法 {} 已运行'.format(func_name.__name__))
            except Exception as e:
                print('进程报错:', e)
        pool.close()
        pool.join()
        print('所有进程方法执行结束')


def push_feishu_msg(msg) -> None:
    """
    推送消息到飞书
    """

    feishu_url = get_java_properties().get("push.feishu.url")
    try:
        # 发送POST请求
        response = requests.post(
            url=feishu_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(msg),
            timeout=10
        )

        print(f"Response status code: {response.status_code}")
        if response.text:
            print(f"Response content: {response.text}")

    except Exception as e:
        print(f"Error sending message: {str(e)}")
        import traceback
        traceback.print_exc()


def get_java_properties():
    # 获取 public_func.py 的所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 从 public_func.py 的目录出发，向上回溯到 stream-common
    prop_path = os.path.join(
        current_dir,
        "..", "stream-common", "src", "main", "resources", "filter", "common-config.properties.prod"
    )
    # 标准化路径（处理 "../"）
    prop_path = os.path.normpath(prop_path)
    with open(prop_path, "rb") as f:
        return javaproperties.load(f)



def submit_seatunnel_restful_v2_job(config_path, host='http://cdh03:14321/submit-job/upload', job_name=None):
    """
    提交作业配置文件到服务器
    :param config_path: 配置文件路径
    :param host: 服务器地址 (默认: http://cdh03:14321/submit-job/upload)
    :param job_name: 作业名称 (可选)
    :return: 服务器响应数据
    """
    # 验证文件存在
    if not os.path.isfile(config_path):
        error_msg = f"配置文件不存在: {config_path}"
        log.error(error_msg)
        raise FileNotFoundError(error_msg)

    # 验证文件后缀
    valid_extensions = ('.json', '.conf', '.config')
    if not config_path.lower().endswith(valid_extensions):
        error_msg = f"文件类型无效: {config_path} (仅支持 {valid_extensions} 后缀)"
        log.error(error_msg)
        raise ValueError(error_msg)

    # 构造请求参数
    url = f"{host.rstrip('/')}/submit-job/upload"
    files = {'config_file': open(config_path, 'rb')}
    data = {'jobName': job_name} if job_name else {}
    log.debug(f"请求URL: {url}")
    log.debug(f"请求数据: {data}")
    log.info(f"正在提交配置文件: {os.path.basename(config_path)}")

    try:
        response = requests.post(url, data=data, files=files)
        response.raise_for_status()
        log.debug(f"响应状态码: {response.status_code}")
        log.debug(f"响应内容: {response.text[:200]}...")
        response.raise_for_status()
        result = response.json()
        log.info(f"作业提交成功! Job ID: {result['jobId']}, Job Name: {result['jobName']}")
    except requests.exceptions.RequestException as e:
        error_msg = f"请求失败: {str(e)}"
        log.error(error_msg, exc_info=True)
        raise ConnectionError(error_msg)
    except ValueError as e:
        error_msg = f"响应解析失败: {str(e)}"
        log.error(error_msg, exc_info=True)
    finally:
        files['config_file'].close()
        log.debug("配置文件句柄已关闭")






