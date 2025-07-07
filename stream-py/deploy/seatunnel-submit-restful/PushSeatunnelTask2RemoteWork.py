import requests
import os
import logging


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


if __name__ == '__main__':
    # sync_mysql8_to_hive3_bigdata_offline_ws_ods_activity_info_tbl_conf = '../../../script/seatunnel-conf/batch/bigdata_offline_ws/sync_mysql8_to_hive3_bigdata_offline_ws_ods_activity_info_tbl.conf'
    sync_mysql8_to_hive3_bigdata_offline_ws_ods_activity_rule_tbl_conf = '../../../script/seatunnel-conf/batch/bigdata_offline_ws/sync_mysql8_to_hive3_bigdata_offline_ws_ods_base_dic_tbl.conf'

    submit_seatunnel_restful_v2_job(sync_mysql8_to_hive3_bigdata_offline_ws_ods_activity_rule_tbl_conf, job_name='sync_mysql8_to_hive3_bigdata_offline_ws_ods_activity_info_tbl')
