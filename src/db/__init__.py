"""数据持久化层 (MySQL + SQLAlchemy)。

- base   : Engine / Session / Declarative Base (同步 + 异步)
- models : Task / Report / Paper 表结构
- crud   : 常用增删改查封装 (同步版供 Celery worker, 异步版供 FastAPI)

DDL 的创建与演进由 Alembic 管理 (见项目根目录 alembic/)。
"""
