"""跨进程事件总线 (基于 Redis Pub/Sub)。

解决的问题：
原项目 SSE 依赖「进程内 asyncio.Queue」，一旦 Worker 与网关分进程，
网关的 event_generator 就拿不到 Worker 产生的进度。

本模块提供：
1. RedisStateQueue：一个「鸭子类型」队列，公暴与 asyncio.Queue 一致的 async put()。
   Worker 侧把它注入 orchestrator/各 node，所有现有的 `await state_queue.put(...)`
   代码无需修改，会自动 PUBLISH 到 Redis 频道。
2. subscribe_events：网关侧的异步生成器，订阅任务频道并逐条 yield。
3. 人工审核输入的跨进程传递 (push_human_input / await_human_input)。
"""

import asyncio
from typing import AsyncIterator, Optional

from src.core.state_models import BackToFrontData
from src.infra import redis_client, settings
from src.infra.redis_client import events_channel, input_list_key
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__)

# 结束哨兵：订阅端收到后关闭 SSE
_DONE_STATES = {"finished"}


class RedisStateQueue:
    """兼容 asyncio.Queue.put 接口的 Redis Pub/Sub 发布器。

    只实现业务代码用到的 `await queue.put(BackToFrontData)`，
    从而在不改动 orchestrator / 各 node 的前提下实现跨进程推送。
    """

    def __init__(self, task_id: str):
        self.task_id = task_id
        self._channel = events_channel(task_id)
        self._redis = redis_client.get_async_redis()

    async def put(self, item: BackToFrontData) -> None:
        try:
            payload = item.model_dump_json()
        except AttributeError:
            # 防御：万一传入的不是 BackToFrontData
            payload = BackToFrontData(step="unknown", state="unknown", data=str(item)).model_dump_json()
        await self._redis.publish(self._channel, payload)

    # 保留 get 接口以防某些调用方期望双向队列（本项目不使用）
    async def get(self):  # pragma: no cover - 不会被调用
        raise NotImplementedError("RedisStateQueue 仅用于发布端")


async def subscribe_events(task_id: str) -> AsyncIterator[str]:
    """订阅某任务的进度事件，逐条 yield JSON 字符串，直到收到结束事件。

    网关侧 SSE 使用。为避免「订阅前事件已发出」的竞态，
    调用方应在发起 Celery 任务之前先建立订阅。
    """
    client = redis_client.get_async_redis()
    pubsub = client.pubsub()
    await pubsub.subscribe(events_channel(task_id))
    logger.info("SSE 订阅频道: %s", events_channel(task_id))
    try:
        async for message in pubsub.listen():
            if message is None or message.get("type") != "message":
                continue
            data = message.get("data")
            if data is None:
                continue
            yield data
            # 简易解析结束信号，避免 SSE 连接泄漏
            if _is_finished(data):
                break
    finally:
        try:
            await pubsub.unsubscribe(events_channel(task_id))
            await pubsub.aclose()
        except Exception:  # noqa: BLE001
            pass
        logger.info("SSE 取消订阅: %s", events_channel(task_id))


def _is_finished(raw: str) -> bool:
    import json

    try:
        obj = json.loads(raw)
    except Exception:  # noqa: BLE001
        return False
    return str(obj.get("step", "")).lower() in _DONE_STATES or str(obj.get("state", "")).lower() in _DONE_STATES


# ----------------------------- 人工审核跨进程传递 -----------------------------
async def push_human_input(task_id: str, user_input: str) -> None:
    """网关收到前端输入后，推送到任务的输入队列。"""
    client = redis_client.get_async_redis()
    await client.rpush(input_list_key(task_id), user_input)
    # 输入队列设置过期，避免残留
    await client.expire(input_list_key(task_id), settings.HUMAN_INPUT_TIMEOUT + 60)


async def await_human_input(task_id: str, timeout: Optional[int] = None) -> Optional[str]:
    """Worker 侧阻塞等待人工输入 (BLPOP)，带超时。

    返回 None 表示超时，调用方可据此选择默认行为或报错，
    从而避免原项目中「协程永久阻塞」造成的任务/上下文泄漏。
    """
    client = redis_client.get_async_redis()
    wait = timeout if timeout is not None else settings.HUMAN_INPUT_TIMEOUT
    result = await client.blpop(input_list_key(task_id), timeout=wait)
    if result is None:
        logger.warning("等待人工输入超时 (task_id=%s, timeout=%ss)", task_id, wait)
        return None
    # result = (key, value)
    return result[1]
