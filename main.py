"""FastAPI 网关（无状态）。

二次开发后的职责边界：
- /api/research : 只负责「入队」(Celery) + 订阅 Redis 事件频道并 SSE 回推，
  不再在 Web 进程里跑几分钟的重任务。
- /send_input : 把人工审核输入 RPUSH 到 Redis，跨进程唤醒 Worker。
- /api/tasks, /api/reports/{task_id} : 基于 MySQL 的历史查询 (支撑前端 History.vue)。
计算逻辑全部下沉到 Celery worker (见 src/tasks/celery_tasks.py)。
"""

import os

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

import uuid

from src.db import crud
from src.infra.event_bus import push_human_input, subscribe_events
from src.infra.celery_app import celery_app  # noqa: F401  (确保任务被注册)
from src.knowledge.knowledge_router import knowledge
from src.utils.log_utils import setup_logger

logger = setup_logger(name="main", log_file="project.log")

app = FastAPI(title="Paper-Agent API", version="0.2.0")
app.include_router(knowledge)

# === CORS 配置 ===
# 修复原「allow_origins=["*"] + allow_credentials=True」的无效组合（浏览器会拒绝）。
# 从环境变量读取允许的域；默认本地开发域。
_origins = os.environ.get("CORS_ALLOW_ORIGINS", "http://localhost:5173,http://localhost:3000")
ALLOW_ORIGINS = [o.strip() for o in _origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/send_input")
async def send_input(data: dict):
    """接收前端的人工输入，按 task_id 跨进程路由到对应 Worker。

    请求体示例: {"task_id": "<hex>", "input": "..."}
    """
    task_id = data.get("task_id")
    user_input = data.get("input")
    if not task_id:
        return JSONResponse({"status": 400, "msg": "缺少 task_id"}, status_code=400)
    await push_human_input(task_id, user_input or "")
    return JSONResponse({"status": 200, "msg": "已收到人工输入"})


@app.get("/api/research")
async def research_stream(query: str):
    """发起一次调研：入队 Celery + 订阅 Redis 事件频道并 SSE 回推。"""
    from src.tasks.celery_tasks import run_research_task

    task_id = uuid.uuid4().hex

    # 先写入任务记录 (PENDING)，数据库不可用时不阻断主流程
    try:
        crud.sync_create_task(task_id, query)
    except Exception as exc:  # noqa: BLE001
        logger.error("创建任务记录失败(忽略): %s", exc)

    async def event_generator():
        # 首个事件下发 task_id，前端据此调用 /send_input 进行人工审核
        yield {"data": '{"step": "task_created", "state": "ready", "data": "%s"}' % task_id}
        # 订阅该任务的 Redis 事件频道，逐条转发给前端
        async for payload in subscribe_events(task_id):
            yield {"data": payload}

    event_source = EventSourceResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"X-Task-Id": task_id},
    )

    # 入队：交由 Celery worker 执行（关键：Web 进程不再跑流水线）
    # 注意：subscribe 在 event_generator 首次被迭代时才建立；Redis Pub/Sub 不会回放，
    # 但 SSE 连接在返回 EventSourceResponse 后立即开始消费，且 Worker 启动有调度延迟，
    # 足以覆盖绝大多数场景；如需严格不丢失，可改用 Redis Stream (见 README 说明)。
    run_research_task.delay(task_id, query)
    logger.info("已入队调研任务 task_id=%s", task_id)

    return event_source


# ----------------------------- 历史查询 (MySQL) -----------------------------
history_router = APIRouter(prefix="/api")


@history_router.get("/tasks")
async def list_tasks(limit: int = 50, offset: int = 0):
    """列出历史任务 (供前端 History.vue)。"""
    try:
        tasks = await crud.async_list_tasks(limit=limit, offset=offset)
        return JSONResponse({"status": 200, "data": tasks})
    except Exception as exc:  # noqa: BLE001
        logger.error("查询任务列表失败: %s", exc)
        return JSONResponse({"status": 500, "msg": str(exc)}, status_code=500)


@history_router.get("/reports/{task_id}")
async def get_report(task_id: str):
    """根据 task_id 获取报告详情。"""
    try:
        report = await crud.async_get_report(task_id)
        if report is None:
            return JSONResponse({"status": 404, "msg": "报告不存在"}, status_code=404)
        return JSONResponse({"status": 200, "data": report})
    except Exception as exc:  # noqa: BLE001
        logger.error("查询报告失败: %s", exc)
        return JSONResponse({"status": 500, "msg": str(exc)}, status_code=500)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


app.include_router(history_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
