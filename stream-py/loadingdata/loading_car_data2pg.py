import json
from tqdm import tqdm
import psycopg2
import uuid
from psycopg2.extras import execute_values

car_file_data_path = '/Users/zhouhan/bigdata_data/gov_car_data/car_data.txt'

DB_CONFIG = {
    'host': '10.160.60.14',
    'port': 5432,
    'database': 'spider_db',
    'user': 'postgres',
    'password': 'Zh1028,./'
}


def process_large_json_file(file_path, batch_size=10000):
    """
    参数:
        file_path: 文件路径
        batch_size: 每批处理的行数，用于控制内存占用
    """
    # 创建数据库连接
    conn = psycopg2.connect(**DB_CONFIG)

    # 存储解析后的数据
    data_batch = []
    total_processed = 0

    # 获取文件总行数，用于显示进度（可选）
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
    except:
        total_lines = None
        print("无法获取文件总行数，将不显示进度条")

    # 逐行读取并解析
    with open(file_path, 'r', encoding='utf-8') as f:
        # mininterval参数确保平滑更新
        with tqdm(total=total_lines, desc="处理进度", mininterval=0.5) as pbar:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    json_data = json.loads(line)
                    data_batch.append(json_data)

                    if len(data_batch) >= batch_size:
                        process_batch(data_batch, conn)
                        total_processed += len(data_batch)
                        data_batch = []
                        pbar.update(batch_size)

                except json.JSONDecodeError as e:
                    pbar.write(f"第{line_num}行解析错误: {str(e)}")
                except Exception as e:
                    pbar.write(f"第{line_num}行处理错误: {str(e)}")

            # 处理剩余的数据
            if data_batch:
                process_batch(data_batch, conn)
                total_processed += len(data_batch)
                pbar.update(len(data_batch))

    # 关闭数据库连接
    conn.close()
    print(f"\n处理完成，共处理{total_processed}行数据")


def process_batch(batch_data, conn):
    """处理每一批解析后的JSON数据并写入数据库"""
    # 准备批量插入数据
    insert_data = []

    for record in batch_data:
        # 生成唯一msid
        msid = uuid.uuid4().hex

        # 提取字段
        upexg = record.get('UPEXGMSGREALLOCATION')
        data_type = record.get('DATATYPE')
        data_field = record.get('DATA')
        register = record.get('UPEXGMSGREGISTER')
        vehicle_no = record.get('VEHICLENO')
        data_len = record.get('DATALEN')
        vehicle_color = record.get('VEHICLECOLOR')

        # 处理None值
        # register = str(register) if register is not None else None

        insert_data.append((
            msid,
            json.dumps(upexg) if upexg else None,
            data_type,
            data_field,
            json.dumps(register) if register else None,
            vehicle_no,
            data_len,
            vehicle_color
        ))

    # 构建SQL插入语句
    insert_sql = """
        INSERT INTO source_data_car_info_message_dtl (
            msid, 
            UPEXGMSGREALLOCATION, 
            DATATYPE, 
            data, 
            UPEXGMSGREGISTER, 
            VEHICLENO, 
            DATALEN, 
            VEHICLECOLOR
        ) VALUES %s
    """

    try:
        # 创建游标并执行批量插入
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                insert_sql,
                insert_data,
                page_size=len(insert_data))
            conn.commit()

    except Exception as e:
        conn.rollback()
        # 使用tqdm.write确保不会干扰进度条
        tqdm.write(f"数据库插入错误: {str(e)}")


if __name__ == "__main__":
    car_file_data_path_1 = '/Users/zhouhan/bigdata_data/gov_car_data/car_data.txt'
    car_file_data_path_2 = '/Users/zhouhan/bigdata_data/gov_car_data/GPS_809_data.txt'
    process_large_json_file(car_file_data_path_2, batch_size=20000)
