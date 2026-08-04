from __future__ import annotations

import asyncio
import logging
import time
import uuid

from redis.exceptions import RedisError as RedisClientError
from sqlalchemy import select

from app.dispatch.service import trigger_dispatch
from app.domain import create_task_log
from common.enums.task_status import TaskStatus
from common.exception.base import StatusNotAllowedError, TaskNotFoundError
from common.utils import from_json_text
from core.conf import settings
from database.db import SessionLocal
from database.models import WindTaskRecord
from database.redis import close_redis, get_redis
from scheduler.queue import TaskClaim, TaskQueue


logger = logging.getLogger(__name__)


class SchedulerWorker:
    """Single-task Redis consumer that reuses the existing dispatch service."""

    def __init__(self, worker_id: str | None = None) -> None:
        self.redis = get_redis()
        self.queue = TaskQueue(self.redis)
        self.worker_id = worker_id or f"scheduler-{uuid.uuid4().hex[:8]}"
        self._last_reconcile_at = 0.0
        self._last_assigned_reconcile_at = 0.0

    async def run(self) -> None:
        await self.redis.ping()
        logger.info("调度 Worker 已启动：%s", self.worker_id)
        while True:
            try:
                await self.queue.requeue_expired(settings.scheduler_requeue_batch_size)
                await self.queue.promote_due_retries(settings.scheduler_requeue_batch_size)
                await self._reconcile_pending_tasks_if_due()
                await self._reconcile_assigned_tasks_if_due()

                claim = await self.queue.claim(self.worker_id)
                if claim is None:
                    await asyncio.sleep(settings.scheduler_poll_interval_seconds)
                    continue
                await self._process(claim)
            except asyncio.CancelledError:
                raise
            except RedisClientError:
                logger.exception("调度 Worker 访问 Redis 失败")
                await asyncio.sleep(max(settings.scheduler_poll_interval_seconds, 1))
            except Exception:
                logger.exception("调度 Worker 主循环异常")
                await asyncio.sleep(max(settings.scheduler_poll_interval_seconds, 1))

    async def _process(self, claim: TaskClaim) -> None:
        try:
            async with SessionLocal() as db:
                result = await trigger_dispatch(db, claim.task_id, None, False)
        except asyncio.CancelledError:
            raise
        except (TaskNotFoundError, StatusNotAllowedError):
            logger.info("任务无需继续调度，直接确认：task_id=%s", claim.task_id)
            await self.queue.ack(claim.task_id)
            return
        except Exception as exc:
            logger.exception("任务调度失败：task_id=%s, attempt=%s", claim.task_id, claim.attempt)
            if claim.attempt >= settings.scheduler_max_retries:
                await self._suspend_after_retries(claim.task_id, str(exc))
                await self.queue.ack(claim.task_id)
            else:
                delay = min(
                    settings.scheduler_retry_base_seconds * (2 ** (claim.attempt - 1)),
                    settings.scheduler_retry_max_seconds,
                )
                await self.queue.schedule_retry(claim.task_id, delay, str(exc))
            return

        await self.queue.ack(claim.task_id)
        logger.info(
            "任务调度成功：task_id=%s, agv_id=%s, status=%s",
            claim.task_id,
            result.get("agvId"),
            result.get("status"),
        )

    async def _suspend_after_retries(self, task_id: int, error: str) -> None:
        async with SessionLocal() as db:
            task = await db.scalar(select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update())
            if not task:
                return
            if task.status in {TaskStatus.PENDING_ASSIGN, TaskStatus.SUSPENDED}:
                task.status = TaskStatus.SUSPENDED
                task.ended_reason = f"调度重试耗尽：{error[:500]}"
                create_task_log(db, task.id, task.ended_reason, level="ERROR")
                await db.commit()

    async def _reconcile_pending_tasks_if_due(self) -> None:
        now = time.monotonic()
        if now - self._last_reconcile_at < settings.scheduler_reconcile_interval_seconds:
            return
        self._last_reconcile_at = now

        async with SessionLocal() as db:
            tasks = (
                await db.scalars(
                    select(WindTaskRecord)
                    .where(WindTaskRecord.status == TaskStatus.PENDING_ASSIGN, WindTaskRecord.is_del == 0)
                    .order_by(WindTaskRecord.priority.desc(), WindTaskRecord.created_on.asc())
                    .limit(settings.scheduler_reconcile_batch_size)
                )
            ).all()

        for task in tasks:
            await self.queue.enqueue(task.id, task.priority, task.created_on)

    async def _reconcile_assigned_tasks_if_due(self) -> None:
        now = time.monotonic()
        if now - self._last_assigned_reconcile_at < settings.scheduler_reconcile_interval_seconds:
            return
        self._last_assigned_reconcile_at = now

        async with SessionLocal() as db:
            tasks = (
                await db.scalars(
                    select(WindTaskRecord)
                    .where(WindTaskRecord.status == TaskStatus.ASSIGNED, WindTaskRecord.is_del == 0)
                    .order_by(WindTaskRecord.priority.desc(), WindTaskRecord.created_on.asc())
                    .limit(settings.scheduler_reconcile_batch_size)
                )
            ).all()
            task_ids = [
                task.id
                for task in tasks
                if not from_json_text(task.variables, {}).get("rmfPublished")
            ]

        for task_id in task_ids:
            try:
                async with SessionLocal() as db:
                    await trigger_dispatch(db, task_id, None, False)
            except Exception:
                logger.exception("failed to republish assigned task to RabbitMQ: task_id=%s", task_id)


async def run_scheduler() -> None:
    """Run the scheduler as a standalone process."""
    try:
        await SchedulerWorker().run()
    finally:
        await close_redis()
