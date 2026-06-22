"""请求级上下文变量。

用于在整条异步流水线内隐式传递 task_id，而不必修改所有函数签名。
典型场景：retrieval_tool 作为 LLM 工具，签名不能加 task_id 参数，
但需要按 task_id 从 Redis 读取任务级临时知识库。

ContextVar 在 asyncio 任务间是隔离的，所以并发任务不会互相覆盖。
"""

import contextvars
from typing import Optional

current_task_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "current_task_id", default=None
)


def set_current_task_id(task_id: Optional[str]) -> None:
    current_task_id.set(task_id)


def get_current_task_id() -> Optional[str]:
    return current_task_id.get()
