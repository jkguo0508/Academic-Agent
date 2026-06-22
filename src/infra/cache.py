"""基于 Redis 的通用缓存与 LLM/Embedding 语义缓存。

主要用途：
- 缓存相同输入的 Embedding / LLM 结果，降低 token 成本与延迟
- 任意可序列化的幂等计算结果缓存

key = sha256(namespace + payload)，值为 JSON，带 TTL。
同时提供同步/异步接口及装饰器。
"""

import functools
import hashlib
import json
from typing import Any, Awaitable, Callable, Optional

from src.infra import redis_client, settings
from src.infra.redis_client import cache_key
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__)


def _digest(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def cache_get(namespace: str, payload: Any) -> Optional[Any]:
    if not settings.LLM_CACHE_ENABLED:
        return None
    try:
        client = redis_client.get_redis()
        raw = client.get(cache_key(namespace, _digest(payload)))
        return json.loads(raw) if raw is not None else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("缓存读取失败(%s): %s", namespace, exc)
        return None


def cache_set(namespace: str, payload: Any, value: Any, ttl: Optional[int] = None) -> None:
    if not settings.LLM_CACHE_ENABLED:
        return
    try:
        client = redis_client.get_redis()
        client.set(
            cache_key(namespace, _digest(payload)),
            json.dumps(value, ensure_ascii=False, default=str),
            ex=ttl or settings.LLM_CACHE_TTL,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("缓存写入失败(%s): %s", namespace, exc)


async def acache_get(namespace: str, payload: Any) -> Optional[Any]:
    if not settings.LLM_CACHE_ENABLED:
        return None
    try:
        client = redis_client.get_async_redis()
        raw = await client.get(cache_key(namespace, _digest(payload)))
        return json.loads(raw) if raw is not None else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("缓存读取失败(%s): %s", namespace, exc)
        return None


async def acache_set(namespace: str, payload: Any, value: Any, ttl: Optional[int] = None) -> None:
    if not settings.LLM_CACHE_ENABLED:
        return
    try:
        client = redis_client.get_async_redis()
        await client.set(
            cache_key(namespace, _digest(payload)),
            json.dumps(value, ensure_ascii=False, default=str),
            ex=ttl or settings.LLM_CACHE_TTL,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("缓存写入失败(%s): %s", namespace, exc)


def cached(namespace: str, ttl: Optional[int] = None):
    """异步函数的语义缓存装饰器。

    以 (args, kwargs) 作为缓存 key 的一部分。适用于 Embedding 等纯函数调用。

    示例::

        @cached("embedding")
        async def embed(text: str) -> list[float]:
            ...
    """

    def decorator(func: Callable[..., Awaitable[Any]]):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            payload = {"args": args, "kwargs": kwargs}
            hit = await acache_get(namespace, payload)
            if hit is not None:
                logger.debug("缓存命中: %s", namespace)
                return hit
            result = await func(*args, **kwargs)
            await acache_set(namespace, payload, result, ttl=ttl)
            return result

        return wrapper

    return decorator
