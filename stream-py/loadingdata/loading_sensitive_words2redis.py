import time
from typing import Awaitable

import redis
from minio import Minio
from loguru import logger

import public_func

minio_endpoint = "10.160.60.17:9000"
minio_secure = False
minio_access_key = 'X7pljEi3steavVn5h3z3'
minio_secret_key = 'KDaSxEyfSEmKiaJDBbJ6RpBxMBp6OwnRbkA8LnKL'
bucket_name = "sensitiveword"
folder_prefix = "sensitiveword/"

word_set = set()

redis_cli = redis.Redis(
    host='10.160.60.17',
    port=6379,
    db=0,
    username="default",
    password='zh1028',
    decode_responses=True
)

minio_client = Minio(
    endpoint=minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=minio_secure
)


def get_minio_sensitive_words():
    objects = minio_client.list_objects(bucket_name=bucket_name)
    for obj in objects:
        if obj.object_name.endswith(".txt"):
            response = minio_client.get_object(bucket_name, obj.object_name)
            content = response.data.decode("utf-8")
            word_set.add(content.replace("\r\n", ','))
            response.close()
            response.release_conn()
    return word_set


def sink_sensitive_words_to_redis():
    """将敏感词同步到Redis数据库"""
    logger.info("开始同步敏感词到Redis...")

    try:
        # 获取敏感词数据
        start_time = time.time()
        logger.debug("正在从MinIO获取敏感词数据...")
        words = get_minio_sensitive_words()
        logger.info(f"成功获取敏感词数据，共 {len(words)} 个原始词条")

        # 清空现有数据
        logger.warning("正在清空Redis敏感词数据库...")
        redis_cli.flushdb()
        logger.info("Redis数据库已清空")

        processed_count = 0
        logger.debug("开始处理敏感词拆分...")

        with redis_cli.pipeline() as pile:
            for idx, combined_words in enumerate(words, 1):
                # 拆分词条
                split_words = [word.strip() for word in combined_words.split(',')]
                valid_words = [word for word in split_words if word]

                # 记录拆分详情
                logger.debug(f"处理第 {idx} 个词条: {combined_words} -> 拆分出 {len(valid_words)} 个有效词")

                # 批量添加
                for word in valid_words:
                    pile.sadd('sensitive_words', word)
                    processed_count += 1

            # 执行批量操作
            logger.info(f"开始提交 {processed_count} 个敏感词到Redis...")
            execute_start = time.time()
            results = pile.execute()
            logger.info(f"提交完成，耗时 {time.time()-execute_start:.2f}s，影响条目数 {sum(results)}")

        # 统计信息
        total_time = time.time() - start_time
        logger.info(f"同步完成！共处理 {len(words)} 个原始词条，入库 {processed_count} 个敏感词，总耗时 {total_time:.2f}秒")
        push_msg = {
            "platform": 'sync_minio_to_redis_sensitive',
            "context": 'loading_sensitive_words2redis.py',
            "total_time": f"{total_time:.2f}",
            "success_count": f"{processed_count}",
            "fail_count": 0,
            "failed_cities": None
        }
        public_func.push_feishu_msg(push_msg)
        return True

    except Exception as e:
        logger.error("敏感词同步失败！", exc_info=True)
        print(e)
        raise


def is_sensitive(word_keys):
    return redis_cli.sismember('sensitive_words', word_keys)


def get_sensitive_words():
    try:
        # 获取全部数据（适合数据量较小的情况）
        ws: Awaitable[set] | set = redis_cli.smembers('sensitive_words')
        print(f"共获取到 {len(ws)} 条敏感词")
        return list(ws)  # 转换为列表

    except redis.exceptions.RedisError as e:
        logger.error(f"读取失败: {str(e)}")
        return []


if __name__ == '__main__':
    sink_sensitive_words_to_redis()
