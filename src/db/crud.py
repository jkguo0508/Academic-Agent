"""任务 / 报告 / 论文的增删改查封装。

- 同步版 (sync_*) 供 Celery worker 使用
- 异步版 (async_*) 供 FastAPI 接口使用
"""

from typing import Any, Dict, List, Optional

from sqlalchemy import select

from src.db.base import AsyncSessionLocal, SessionLocal
from src.db.models import Paper, Report, Task, TaskStatus
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__)


# ============================ 同步 (Celery worker) ============================
def sync_create_task(task_id: str, query: str, max_papers: int = 50) -> None:
    with SessionLocal() as session:
        existing = session.get(Task, task_id) if False else session.execute(
            select(Task).where(Task.task_id == task_id)
        ).scalar_one_or_none()
        if existing is not None:
            return
        session.add(
            Task(task_id=task_id, query=query, max_papers=max_papers, status=TaskStatus.PENDING)
        )
        session.commit()


def sync_update_task_status(
    task_id: str,
    status: TaskStatus,
    *,
    current_step: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    with SessionLocal() as session:
        task = session.execute(
            select(Task).where(Task.task_id == task_id)
        ).scalar_one_or_none()
        if task is None:
            logger.warning("更新状态时未找到任务: %s", task_id)
            return
        task.status = status
        if current_step is not None:
            task.current_step = current_step
        if error is not None:
            task.error = error
        session.commit()


def sync_save_report(
    task_id: str,
    content_markdown: Optional[str],
    *,
    title: Optional[str] = None,
    outline: Optional[str] = None,
) -> None:
    with SessionLocal() as session:
        report = session.execute(
            select(Report).where(Report.task_id == task_id)
        ).scalar_one_or_none()
        if report is None:
            report = Report(task_id=task_id)
            session.add(report)
        report.content_markdown = content_markdown
        report.title = title
        report.outline = outline
        session.commit()


def sync_save_papers(task_id: str, papers: List[Dict[str, Any]]) -> None:
    if not papers:
        return
    with SessionLocal() as session:
        for p in papers:
            authors = p.get("authors")
            if isinstance(authors, list):
                authors = ", ".join(str(a) for a in authors)
            session.add(
                Paper(
                    task_id=task_id,
                    paper_id=str(p.get("paper_id") or ""),
                    title=str(p.get("title") or "")[:1024],
                    authors=authors,
                    url=str(p.get("url") or "")[:1024],
                    published=str(p.get("published") or "")[:32],
                    extra={k: v for k, v in p.items() if k not in ("summary",)},
                )
            )
        session.commit()


# ============================ 异步 (FastAPI) ============================
async def async_list_tasks(limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    async with AsyncSessionLocal() as session:
        rows = (
            await session.execute(
                select(Task).order_by(Task.created_at.desc()).limit(limit).offset(offset)
            )
        ).scalars().all()
        return [
            {
                "task_id": t.task_id,
                "query": t.query,
                "status": t.status.value if hasattr(t.status, "value") else str(t.status),
                "current_step": t.current_step,
                "error": t.error,
                "created_at": t.created_at.isoformat() if t.created_at else None,
                "updated_at": t.updated_at.isoformat() if t.updated_at else None,
            }
            for t in rows
        ]


async def async_get_report(task_id: str) -> Optional[Dict[str, Any]]:
    async with AsyncSessionLocal() as session:
        report = (
            await session.execute(select(Report).where(Report.task_id == task_id))
        ).scalar_one_or_none()
        if report is None:
            return None
        return {
            "task_id": report.task_id,
            "title": report.title,
            "outline": report.outline,
            "content_markdown": report.content_markdown,
            "created_at": report.created_at.isoformat() if report.created_at else None,
        }
