"""ORM 模型：任务 / 报告 / 论文。

设计目标：把原本「内存态、重启即丢」的任务与报告落库，
支撑历史记录查询 (前端 History.vue) 与任务状态机恢复。
"""

import enum
from datetime import datetime

from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base


class TaskStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Task(Base):
    """一次调研任务。与 Celery task_id 一一对应。"""

    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    query: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus), default=TaskStatus.PENDING, nullable=False, index=True
    )
    current_step: Mapped[str] = mapped_column(String(32), default="initializing", nullable=False)
    error: Mapped[str] = mapped_column(Text, nullable=True)
    max_papers: Mapped[int] = mapped_column(Integer, default=50, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    report: Mapped["Report"] = relationship(
        back_populates="task", uselist=False, cascade="all, delete-orphan"
    )
    papers: Mapped[list["Paper"]] = relationship(
        back_populates="task", cascade="all, delete-orphan"
    )


class Report(Base):
    """任务产出的最终 Markdown 报告。"""

    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("tasks.task_id", ondelete="CASCADE"), index=True, nullable=False
    )
    title: Mapped[str] = mapped_column(String(512), nullable=True)
    content_markdown: Mapped[str] = mapped_column(Text(length=4_000_000), nullable=True)
    outline: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    task: Mapped["Task"] = relationship(back_populates="report")


class Paper(Base):
    """任务检索/阅读到的论文元数据 (便于溯源与复用)。"""

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("tasks.task_id", ondelete="CASCADE"), index=True, nullable=False
    )
    paper_id: Mapped[str] = mapped_column(String(128), nullable=True)
    title: Mapped[str] = mapped_column(String(1024), nullable=True)
    authors: Mapped[str] = mapped_column(Text, nullable=True)
    url: Mapped[str] = mapped_column(String(1024), nullable=True)
    published: Mapped[str] = mapped_column(String(32), nullable=True)
    extra: Mapped[dict] = mapped_column(JSON, nullable=True)

    task: Mapped["Task"] = relationship(back_populates="papers")
