"""基于 Redis 的分布式锁。

用于保护跨进程的临界区，例如：
- 创建任务级临时知识库时，避免并发重复创建 / 覆盖
- 任何需要「同一资源全局串行」的场景

实现采用 SET NX PX + 唯一 token + Lua 脚本释放，保证「谁加锁谁释放」。
同时提供同步与异步上下文管理器。
"""

import asyncio
import contextlib
import time
import uuid
from typing import AsyncIterator, Iterator

from src.infra import redis_client
from src.infra.redis_client import lock_key
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__)

# 原子释放：仅当 value 与加锁 token 一致时才删除，避免误释放别人的锁
_RELEASE_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "return redis.call('del', KEYS[1]) else return 0 end"
)


@contextlib.contextmanager
def redis_lock(name: str, *, timeout: float = 30.0, blocking_timeout: float = 10.0,
               retry_interval: float = 0.1) -> Iterator[bool]:
    """同步分布式锁。

    Args:
        name: 锁名 (会加上统一前缀)。
        timeout: 锁的自动过期时间 (秒)，防止持锁者崩溃后死锁。
        blocking_timeout: 获取锁的最大等待时间 (秒)。

    Yields:
        bool: 是否成功获取到锁。
    """
    client = redis_client.get_redis()
    key = lock_key(name)
    token = uuid.uuid4().hex
    px = int(timeout * 1000)
    deadline = time.monotonic() + blocking_timeout
    acquired = False
    while True:
        acquired = bool(client.set(key, token, nx=True, px=px))
        if acquired or time.monotonic() >= deadline:
            break
        time.sleep(retry_interval)
    try:
        if not acquired:
            logger.warning("获取分布式锁超时: %s", key)
        yield acquired
    finally:
        if acquired:
            try:
                client.eval(_RELEASE_LUA, 1, key, token)
            except Exception as exc:  # noqa: BLE001
                logger.error("释放分布式锁失败 %s: %s", key, exc)


@contextlib.asynccontextmanager
async def async_redis_lock(name: str, *, timeout: float = 30.0,
                          blocking_timeout: float = 10.0,
                          retry_interval: float = 0.1) -> AsyncIterator[bool]:
    """异步分布式锁，语义同 redis_lock。"""
    client = redis_client.get_async_redis()
    key = lock_key(name)
    token = uuid.uuid4().hex
    px = int(timeout * 1000)
    deadline = time.monotonic() + blocking_timeout
    acquired = False
    while True:
        acquired = bool(await client.set(key, token, nx=True, px=px))
        if acquired or time.monotonic() >= deadline:
            break
        await asyncio.sleep(retry_interval)
    try:
        if not acquired:
            logger.warning("获取分布式锁超时: %s", key)
        yield acquired
    finally:
        if acquired:
            try:
                await client.eval(_RELEASE_LUA, 1, key, token)
            except Exception as exc:  # noqa: BLE001
                logger.error("释放分布式锁失败 %s: %s", key, exc)
