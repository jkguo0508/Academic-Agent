"""SQLAlchemy engine / session / Base。

提供两套连接：
- 同步 (pymysql)  : 供 Celery worker 与 Alembic 使用
- 异步 (aiomysql) : 供 FastAPI 接口使用
都开启了 pool_pre_ping，避免 MySQL 连接被服务端断开后报 stale 错误。
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from src.infra import settings


class Base(DeclarativeBase):
    """所有 ORM 模型的声明基类。"""
    pass


# ----------------------------- 同步 -----------------------------
sync_engine = create_engine(
    settings.DATABASE_URL_SYNC,
    pool_pre_ping=True,
    pool_recycle=3600,
    future=True,
)
SessionLocal = sessionmaker(bind=sync_engine, autoflush=False, expire_on_commit=False)


# ----------------------------- 异步 -----------------------------
async_engine = create_async_engine(
    settings.DATABASE_URL_ASYNC,
    pool_pre_ping=True,
    pool_recycle=3600,
    future=True,
)
AsyncSessionLocal = async_sessionmaker(bind=async_engine, autoflush=False, expire_on_commit=False)
