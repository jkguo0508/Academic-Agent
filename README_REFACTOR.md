# Paper-Agent 二次开发说明：分布式架构升级

> 本文描述在原版 Paper-Agent 基础上完成的「方案一」重构，目标是把一个「单进程、内存态、请求内跑流水线」的原型，升级为「可水平扩展、跨进程、可持久化」的生产级架构。

## 1. 本次做了什么

| # | 能力 | 技术 | 解决的原生痛点 |
|---|------|------|---------------|
| 1 | 流水线异步化 | **Celery + Redis** | 原本 `asyncio.create_task` 在 Web 进程里跑几分钟的重任务，阻塞、不能水平扩展、重启即丢 |
| 2 | 跨进程实时推送 | **Redis Pub/Sub** | 原本 SSE 依赖进程内 `asyncio.Queue`，Worker 与网关分进程后就拿不到进度 |
| 3 | 数据持久化 | **MySQL + SQLAlchemy + Alembic** | 原本任务/报告全在内存，重启丢失，无历史记录 |
| 4 | 分布式锁 + 缓存 | **Redis** | 原本用全局 `config.set("tmp_db_id")` 传递临时库 id，并发任务会互相覆盖；Embedding 无缓存 |

## 2. 架构变化

### 重构前
```
浏览器 ──HTTP/SSE──> FastAPI 进程
                          └─ asyncio.create_task(orchestrator.run)  # 跟请求同进程
                          └─ asyncio.Queue                          # 进度只在本进程
                          └─ 全局 config / userProxyAgent           # 单例，串扰
```

### 重构后
```
浏览器 ──HTTP/SSE──> FastAPI 网关 (无状态，可多副本)
     │                     │
     │  delay()            └─ SUBSCRIBE pa:events:{task_id} (Redis Pub/Sub) ──> SSE 回推
     ▼                     ▲
  Redis (broker) ──> Celery Worker (可多副本/多机)
                          └─ RedisStateQueue.put ──PUBLISH──> pa:events:{task_id}
                          └─ RedisUserProxyAgent BLPOP pa:input:{task_id}  # 跨进程人工审核
                          └─ 分布式锁 pa:lock:* / 任务级 pa:taskdb:{task_id}
                          └─ 结果落库 MySQL (tasks/reports/papers)
```

## 3. 新增 / 修改的文件

### 新增基础设施 `src/infra/`
- `settings.py` — 统一配置入口 (Redis / MySQL / Celery / 业务参数)
- `redis_client.py` — 同步/异步 Redis 连接池 + key 命名约定
- `celery_app.py` — Celery 应用 (Redis broker/backend)
- `event_bus.py` — **Redis Pub/Sub 事件总线**：`RedisStateQueue`(鸭子类型兼容 `asyncio.Queue.put`)、`subscribe_events`、跨进程人工输入
- `locks.py` — **分布式锁** (SET NX PX + Lua 原子释放，同步/异步)
- `cache.py` — **Redis 语义缓存** (通用 + `@cached` 装饰器)
- `context.py` — ContextVar 传递 task_id (供 LLM 工具等无法传参处)

### 新增数据层 `src/db/`
- `base.py` — 同步(pymysql)/异步(aiomysql) Engine 与 Session
- `models.py` — `Task` / `Report` / `Paper` 三张表
- `crud.py` — 同步(worker) + 异步(FastAPI) 增删改查

### 新增迁移 `alembic/`
- `alembic.ini` / `alembic/env.py` / `alembic/versions/0001_initial.py`

### 新增任务 `src/tasks/celery_tasks.py`
- `run_research_task` — Worker 中运行流水线，结果落库，**无论成败都发 FINISHED** (修复 SSE 泄漏)

### 修改的文件
- `main.py` — `/api/research` 改为「入队 + 订阅 Redis 频道」；`/send_input` 改为 RPUSH；新增 `/api/tasks`、`/api/reports/{task_id}` 历史查询；修复 CORS 非法组合
- `src/core/task_context.py` — 新增 `register()`，支持 worker 注入基于 Redis 的 state_queue/user_proxy
- `src/agents/userproxy_agent.py` — 新增 `RedisUserProxyAgent`(BLPOP 带超时)；原 `WebUserProxyAgent` 加超时防泄漏
- `src/agents/orchestrator.py` — `run()` 返回最终状态供持久化；FINISHED 下放到调用方 finally 统一发
- `src/agents/reading_agent.py` — 用**分布式锁**保护临时库创建，把 db_id 按 task_id 存 Redis (取代全局 config)
- `src/services/retrieval_tool.py` — 按 task_id 从 Redis 读取任务级临时库 (任务隔离)

### 部署
- `Dockerfile` / `docker-compose.yml` (api + worker + redis + mysql + migrate + flower)
- `example.env` (新增 Redis/MySQL/业务参数) / `requirements-refactor.txt`

## 4. 如何运行

### 方式 A：docker-compose (推荐)
```bash
cp example.env .env
# .env 中把 REDIS_HOST=redis、MYSQL_HOST=mysql 打开
docker compose up -d --build
# api: http://localhost:8000  flower: http://localhost:5555
```

### 方式 B：本地开发
```bash
# 1. 启动 Redis / MySQL (本地或 docker)
# 2. 安装依赖
pip install -r requirements-refactor.txt   # 或 poetry install
# 3. 建表
alembic upgrade head
# 4. 启动网关
uvicorn main:app --reload --port 8000
# 5. 另起一个终端启动 worker
celery -A src.infra.celery_app.celery_app worker --loglevel=info --concurrency=4
```

## 5. 调用流程 (前端适配)
1. `GET /api/research?query=...` → SSE，首条事件 `step=task_created` 携带 `task_id`。
2. 需要人工审核时 `POST /send_input {task_id, input}`。
3. 进度通过同一 SSE 流推送，`step=finished` 时关闭。
4. 历史：`GET /api/tasks`、`GET /api/reports/{task_id}`。

## 6. 简历可写的亮点
- 使用 **Celery + Redis** 将耗时的多智能体流水线从 HTTP 请求中剖离，网关无状态可水平扩展，Worker 可独立扩容。
- 基于 **Redis Pub/Sub** 重构 SSE，解决跨进程实时进度推送；设计 `RedisStateQueue` 鸭子类型适配器，零侵入复用原有节点代码。
- 使用 **SQLAlchemy 2.0 (async) + Alembic** 完成任务/报告持久化与版本化迁移。
- 自研 **Redis 分布式锁 (SET NX PX + Lua)** 与任务级 key 隔离，修复并发任务间临时知识库串扰的严重缺陷。
- 修复了三个稳定性 Bug：临时库并发覆盖、SSE 连接泄漏、人工审核无超时永久阻塞。
