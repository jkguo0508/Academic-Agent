"""
请求级任务上下文与注册表。

原本用于替换 main.py 中的全局单例 (state_queue / userProxyAgent)，
使每个 /api/research 请求拥有独立的 state_queue 与 user_proxy。

二次开发后，任务真正的执行转移到 Celery worker 进程：
- worker 使用 register() 把「基于 Redis 的」 state_queue / user_proxy 注册进来，
  使 search_node 等节点仍可通过 task_registry.get(task_id) 拿到本任务专属代理。
- create() 保留向后兼容（单进程/本地调试场景）。
"""

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from src.utils.log_utils import setup_logger

logger = setup_logger(__name__)


@dataclass
class TaskContext:
    """单个请求的运行上下文。

    state_queue / user_proxy 使用 Any 类型，以同时兼容：
    - 本地实现：asyncio.Queue + WebUserProxyAgent
    - 分布式实现：RedisStateQueue + RedisUserProxyAgent
    """

    task_id: str
    state_queue: Any
    user_proxy: Any


class TaskRegistry:
    """按 task_id 维度管理任务上下文的注册表（进程内）。"""

    def __init__(self) -> None:
        self._contexts: Dict[str, TaskContext] = {}

    def create(self, task_id: Optional[str] = None) -> TaskContext:
        """本地/单进程场景：创建基于 asyncio.Queue 的上下文（向后兼容）。"""
        from src.agents.userproxy_agent import WebUserProxyAgent

        task_id = task_id or uuid.uuid4().hex
        ctx = TaskContext(
            task_id=task_id,
            state_queue=asyncio.Queue(),
            user_proxy=WebUserProxyAgent(f"user_proxy_{task_id}"),
        )
        self._contexts[task_id] = ctx
        logger.info(f"创建任务上下文: {task_id} (当前活跃任务数={len(self._contexts)})")
        return ctx

    def register(self, task_id: str, state_queue: Any, user_proxy: Any) -> TaskContext:
        """分布式场景：worker 注入自定义的 state_queue / user_proxy。"""
        ctx = TaskContext(task_id=task_id, state_queue=state_queue, user_proxy=user_proxy)
        self._contexts[task_id] = ctx
        logger.info(f"注册任务上下文: {task_id} (当前活跃任务数={len(self._contexts)})")
        return ctx

    def get(self, task_id: Optional[str]) -> Optional[TaskContext]:
        """根据 task_id 获取上下文，不存在返回 None。"""
        if not task_id:
            return None
        return self._contexts.get(task_id)

    def remove(self, task_id: Optional[str]) -> None:
        """移除并释放任务上下文。"""
        if not task_id:
            return
        ctx = self._contexts.pop(task_id, None)
        if ctx is not None:
            logger.info(f"移除任务上下文: {task_id} (剩余活跃任务数={len(self._contexts)})")


# 全局唯一的注册表实例（管理的是 per-task 状态，本身无业务串扰问题）
task_registry = TaskRegistry()
