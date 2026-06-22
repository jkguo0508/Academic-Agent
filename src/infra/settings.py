"""基础设施统一配置入口。

从环境变量读取 Redis / MySQL / Celery 的连接信息，提供合理默认值，
使本地开发与容器化部署 (docker-compose) 可以无缝切换。

所有新增中间件的配置都集中在这里，避免散落在各个模块。
"""

import os


def _env(key: str, default: str) -> str:
    value = os.environ.get(key)
    return value if value not in (None, "") else default


# ----------------------------- Redis -----------------------------
REDIS_HOST = _env("REDIS_HOST", "localhost")
REDIS_PORT = int(_env("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or None

# 用途分库，避免 key 互相干扰
REDIS_DB_BROKER = int(_env("REDIS_DB_BROKER", "0"))      # Celery broker
REDIS_DB_BACKEND = int(_env("REDIS_DB_BACKEND", "1"))    # Celery result backend
REDIS_DB_APP = int(_env("REDIS_DB_APP", "2"))            # 事件总线 / 锁 / 缓存


def _redis_url(db: int) -> str:
    auth = f":{REDIS_PASSWORD}@" if REDIS_PASSWORD else ""
    return f"redis://{auth}{REDIS_HOST}:{REDIS_PORT}/{db}"


REDIS_URL_APP = _redis_url(REDIS_DB_APP)
CELERY_BROKER_URL = _env("CELERY_BROKER_URL", _redis_url(REDIS_DB_BROKER))
CELERY_RESULT_BACKEND = _env("CELERY_RESULT_BACKEND", _redis_url(REDIS_DB_BACKEND))


# ----------------------------- MySQL -----------------------------
MYSQL_HOST = _env("MYSQL_HOST", "localhost")
MYSQL_PORT = int(_env("MYSQL_PORT", "3306"))
MYSQL_USER = _env("MYSQL_USER", "paper_agent")
MYSQL_PASSWORD = _env("MYSQL_PASSWORD", "paper_agent")
MYSQL_DB = _env("MYSQL_DB", "paper_agent")

# 异步驱动 (aiomysql) 供 FastAPI 使用；同步驱动 (pymysql) 供 Celery worker / Alembic 使用
DATABASE_URL_ASYNC = _env(
    "DATABASE_URL_ASYNC",
    f"mysql+aiomysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4",
)
DATABASE_URL_SYNC = _env(
    "DATABASE_URL_SYNC",
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4",
)


# ----------------------------- 业务参数 -----------------------------
# 人工审核等待超时 (秒)，避免 Worker 协程永久阻塞造成泄漏
HUMAN_INPUT_TIMEOUT = int(_env("HUMAN_INPUT_TIMEOUT", "600"))
# LLM / Embedding 缓存默认 TTL (秒)
LLM_CACHE_TTL = int(_env("LLM_CACHE_TTL", "86400"))
# 是否启用 LLM 缓存
LLM_CACHE_ENABLED = _env("LLM_CACHE_ENABLED", "true").lower() in ("1", "true", "yes", "y")
# 事件/输入 key 的统一前缀
KEY_PREFIX = _env("PA_KEY_PREFIX", "pa")
