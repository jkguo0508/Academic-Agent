"""Celery 任务：在 Worker 中运行多智能体调研流水线。

这是「把流水线从请求里剖离出去」的落地点：
FastAPI 只负责入队 (run_research_task.delay)，Worker 负责执行。

Worker 中的执行流程：
1. 写入/更新 MySQL 任务状态 (持久化)
2. 构造 RedisStateQueue (进度事件走 Redis Pub/Sub) 与 RedisUserProxyAgent (人工审核走 Redis)
3. 运行 LangGraph 流水线
4. 报告 / 论文落库，无论成败都 PUBLISH FINISHED (避免 SSE 泄漏)
"""

import asyncio
import traceback
from typing import Optional

from src.core.state_models import BackToFrontData, ExecutionState
from src.core.task_context import task_registry
from src.db import crud
from src.db.models import TaskStatus
from src.infra import redis_client
from src.infra.celery_app import celery_app
from src.infra.event_bus import RedisStateQueue
from src.utils.log_utils import setup_logger

logger = setup_logger(name="celery_tasks", log_file="worker.log")


@celery_app.task(bind=True, name="paper_agent.run_research")
def run_research_task(self, task_id: str, query: str, max_papers: int = 50) -> dict:
    """Celery 同步入口：在独立事件循环中执行异步流水线。"""
    logger.info("[worker] 接收调研任务 task_id=%s query=%s", task_id, query)
    return asyncio.run(_run_async(task_id, query, max_papers))


async def _run_async(task_id: str, query: str, max_papers: int) -> dict:
    # 延迟导入，避免在模块加载期就初始化重量级依赖
    from src.agents.orchestrator import PaperAgentOrchestrator
    from src.agents.userproxy_agent import RedisUserProxyAgent

    # 设置请求级上下文 (供 retrieval_tool 等无法传参的地方读取 task_id)
    from src.infra.context import set_current_task_id
    set_current_task_id(task_id)

    state_queue = RedisStateQueue(task_id)

    # 在 worker 进程内注册任务上下文，使 search_node 能通过 task_registry.get(task_id)
    # 找到本任务专属的 (基于 Redis 的) user_proxy。
    user_proxy = RedisUserProxyAgent(f"user_proxy_{task_id}", task_id=task_id)
    task_registry.register(task_id=task_id, state_queue=state_queue, user_proxy=user_proxy)

    try:
        crud.sync_update_task_status(task_id, TaskStatus.RUNNING, current_step="searching")
    except Exception as exc:  # noqa: BLE001 - 数据库不可用时不应阻断业务
        logger.error("更新任务状态失败(忽略): %s", exc)

    final_value = None
    error_msg: Optional[str] = None
    try:
        orchestrator = PaperAgentOrchestrator(state_queue=state_queue, task_id=task_id)
        final_value = await orchestrator.run(user_request=query, max_papers=max_papers)
    except Exception as exc:  # noqa: BLE001
        error_msg = f"{exc}\n{traceback.format_exc()}"
        logger.error("[worker] 任务执行失败 task_id=%s: %s", task_id, error_msg)
        # 把错误也推送给前端
        try:
            await state_queue.put(
                BackToFrontData(step=ExecutionState.FAILED, state="error", data=str(exc))
            )
        except Exception:  # noqa: BLE001
            pass
    finally:
        # 无论成败，都保证发出 FINISHED，防止 SSE 泄漏 (修复原 reading_node 无 try/except 的问题)
        try:
            await state_queue.put(
                BackToFrontData(step=ExecutionState.FINISHED, state="finished", data=None)
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("发送 FINISHED 失败: %s", exc)
        # 清理任务级 Redis 资源 (临时知识库 id / 输入队列)
        await _cleanup_task_keys(task_id)
        task_registry.remove(task_id)

    # 持久化结果
    await _persist_result(task_id, final_value, error_msg)
    return {"task_id": task_id, "status": "failed" if error_msg else "completed"}


async def _persist_result(task_id: str, final_value, error_msg: Optional[str]) -> None:
    try:
        if error_msg:
            crud.sync_update_task_status(
                task_id, TaskStatus.FAILED, current_step="failed", error=error_msg[:4000]
            )
            return

        report_md = getattr(final_value, "report_markdown", None) if final_value else None
        outline = getattr(final_value, "outline", None) if final_value else None
        papers = getattr(final_value, "search_results", None) if final_value else None

        crud.sync_save_report(task_id, report_md, title=None, outline=outline)
        if papers:
            crud.sync_save_papers(task_id, papers)
        crud.sync_update_task_status(task_id, TaskStatus.COMPLETED, current_step="completed")
    except Exception as exc:  # noqa: BLE001
        logger.error("持久化结果失败 task_id=%s: %s", task_id, exc)


async def _cleanup_task_keys(task_id: str) -> None:
    try:
        client = redis_client.get_async_redis()
        await client.delete(
            redis_client.task_db_id_key(task_id),
            redis_client.input_list_key(task_id),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("清理任务键失败 task_id=%s: %s", task_id, exc)
