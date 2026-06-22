"""基础设施层 (infra)。

本包是二次开发新增的「分布式化」基础设施，集中管理与 Redis / Celery 等
外部中间件的交互，使业务代码与具体中间件解耦：

- redis_client : Redis 连接管理 (同步 + 异步, 连接池单例)
- event_bus    : 基于 Redis Pub/Sub 的跨进程事件总线 + SSE 适配队列
- locks        : 基于 Redis 的分布式锁
- cache        : 基于 Redis 的通用缓存 / LLM 语义缓存
- celery_app   : Celery 应用 (Redis 作 broker + result backend)
"""
