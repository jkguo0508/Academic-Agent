"""Celery application (Redis broker + result backend).

This is the core of feature #1: moving the long-running multi-agent pipeline
out of the FastAPI request and into a dedicated worker pool, so the web layer
and the compute layer are decoupled and workers can scale horizontally.

Start a worker with::

    celery -A src.infra.celery_app.celery_app worker --loglevel=info --concurrency=4
"""

from celery import Celery

from src.infra import settings

celery_app = Celery(
    "paper_agent",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["src.tasks.celery_tasks"],
)

celery_app.conf.update(
    task_track_started=True,
    task_acks_late=True,                 # only ack after the task finishes; survive worker crash
    worker_prefetch_multiplier=1,        # fair dispatch for long tasks
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Shanghai",
    enable_utc=False,
    result_expires=3600,
    broker_connection_retry_on_startup=True,
)
