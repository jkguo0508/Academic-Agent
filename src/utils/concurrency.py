"""
并发治理工具模块。

提供:
- 基于 asyncio.Semaphore 的限流 + 分批并发执行 (gather_in_batches)
- 基于 tenacity 的异步重试 (run_with_retry)
- 从配置读取 max_workers / batch_size / 重试参数的辅助函数

配置来源 (system_params.yaml):
- concurrency.max_workers   : 并发上限(信号量大小)
- performance.batch_size     : 单批任务数量
- request.max_retries        : 最大重试次数
- request.retry_delay        : 重试基础退避(秒)
"""

import asyncio
from typing import Any, Awaitable, Callable, List, Optional, Sequence, Tuple, Type

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.core.config import config
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__)

# 工厂类型：调用后返回一个新的可等待对象(每次重试都需要一个全新的协程)
AwaitableFactory = Callable[[], Awaitable[Any]]


def get_max_workers() -> int:
    """并发上限，对应 concurrency.max_workers。"""
    return max(1, config.get_int("concurrency.max_workers", 4))


def get_batch_size() -> int:
    """单批任务数量，对应 performance.batch_size。"""
    return max(1, config.get_int("performance.batch_size", 10))


def get_max_retries() -> int:
    """最大重试次数，对应 request.max_retries。"""
    return max(1, config.get_int("request.max_retries", 3))


def get_retry_delay() -> float:
    """重试基础退避(秒)，对应 request.retry_delay。"""
    return max(0.0, config.get_float("request.retry_delay", 1.0))


async def run_with_retry(
    factory: AwaitableFactory,
    *,
    max_attempts: Optional[int] = None,
    exc_types: Tuple[Type[BaseException], ...] = (Exception,),
) -> Any:
    """对一个异步工厂执行带指数退避的重试。

    Args:
        factory: 无参可调用对象，每次调用返回一个**全新**的协程/awaitable。
        max_attempts: 最大尝试次数，默认读取 request.max_retries。
        exc_types: 需要触发重试的异常类型。

    Returns:
        工厂协程的返回值。

    Raises:
        最后一次尝试抛出的异常(reraise=True)。
    """
    attempts = max_attempts or get_max_retries()
    delay = get_retry_delay() or 0.5

    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(attempts),
        wait=wait_exponential(multiplier=delay, min=delay, max=delay * 10),
        retry=retry_if_exception_type(exc_types),
        reraise=True,
    ):
        with attempt:
            return await factory()


async def gather_in_batches(
    factories: Sequence[AwaitableFactory],
    *,
    concurrency_limit: Optional[int] = None,
    batch_size: Optional[int] = None,
    return_exceptions: bool = True,
) -> List[Any]:
    """使用信号量限流 + 分批的方式并发执行一组异步工厂。

    - 信号量大小取 concurrency_limit (默认 concurrency.max_workers)，限制同时在飞的任务数。
    - 分批大小取 batch_size (默认 performance.batch_size)，避免一次性创建海量协程。

    Args:
        factories: 无参工厂列表，每个调用返回一个 awaitable。
        concurrency_limit: 并发上限。
        batch_size: 单批任务数量。
        return_exceptions: 透传给 asyncio.gather，True 时异常作为结果返回而不中断。

    Returns:
        与 factories 顺序一致的结果列表(可能包含异常对象)。
    """
    if not factories:
        return []

    limit = concurrency_limit or get_max_workers()
    bsize = batch_size or get_batch_size()
    semaphore = asyncio.Semaphore(limit)

    async def _guarded(factory: AwaitableFactory) -> Any:
        async with semaphore:
            return await factory()

    results: List[Any] = []
    total = len(factories)
    for start in range(0, total, bsize):
        batch = factories[start : start + bsize]
        logger.info(
            f"并发执行批次 {start // bsize + 1}: {len(batch)} 个任务 (limit={limit}, batch_size={bsize})"
        )
        batch_results = await asyncio.gather(
            *[_guarded(f) for f in batch], return_exceptions=return_exceptions
        )
        results.extend(batch_results)
    return results
