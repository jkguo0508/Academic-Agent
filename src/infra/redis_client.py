"""Redis 连接管理。

同时提供同步 (redis) 与异步 (redis.asyncio) 客户端，均使用连接池单例：

- FastAPI (异步) 使用 get_async_redis()
- Celery worker (同步上下文) 使用 get_redis()；worker 内部跑 asyncio 时也可用 get_async_redis()

连接池懒加载，首次使用时创建；跨进程不共享，但各进程内复用同一池。
"""

from typing import Optional

import redis
import redis.asyncio as aioredis

from src.infra import settings
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__)

_sync_pool: Optional[redis.ConnectionPool] = None
_async_pool: Optional[aioredis.ConnectionPool] = None


def get_redis() -> redis.Redis:
    """获取同步 Redis 客户端 (decode_responses=True)。"""
    global _sync_pool
    if _sync_pool is None:
        _sync_pool = redis.ConnectionPool.from_url(
            settings.REDIS_URL_APP, decode_responses=True
        )
        logger.info("初始化同步 Redis 连接池: %s", settings.REDIS_URL_APP)
    return redis.Redis(connection_pool=_sync_pool)


def get_async_redis() -> aioredis.Redis:
    """获取异步 Redis 客户端 (decode_responses=True)。"""
    global _async_pool
    if _async_pool is None:
        _async_pool = aioredis.ConnectionPool.from_url(
            settings.REDIS_URL_APP, decode_responses=True
        )
        logger.info("初始化异步 Redis 连接池: %s", settings.REDIS_URL_APP)
    return aioredis.Redis(connection_pool=_async_pool)


# ----------------------------- key 命名约定 -----------------------------
_P = settings.KEY_PREFIX


def events_channel(task_id: str) -> str:
    """任务进度事件的 Pub/Sub 频道。"""
    return f"{_P}:events:{task_id}"


def input_list_key(task_id: str) -> str:
    """人工审核输入的阻塞队列 (BLPOP)。"""
    return f"{_P}:input:{task_id}"


def task_db_id_key(task_id: str) -> str:
    """任务级临时知识库 ID，取代全局 config 的 tmp_db_id。"""
    return f"{_P}:taskdb:{task_id}"


def lock_key(name: str) -> str:
    return f"{_P}:lock:{name}"


def cache_key(namespace: str, digest: str) -> str:
    return f"{_P}:cache:{namespace}:{digest}"
