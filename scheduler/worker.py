from __future__ import annotations

import asyncio
import logging
import time
import uuid

from redis.exceptions import RedisError as RedisClientError
from sqlalchemy import select

from app.dispatch.service import trigger_dispatch
from app.domain import create_task_log, mark_robot_idle, release_reserved_map_nodes
from app.map.service import run_map_cache_listener
from common.enums.task_status import TaskStatus
from common.exception.base import MapVersionUnavailableError, StatusNotAllowedError, TaskNotFoundError
from common.utils import from_json_text
from core.conf import settings
from database.db import SessionLocal
from database.models import WindTaskRecord
from database.redis import close_redis, get_redis
from scheduler.queue import TaskClaim, TaskQueue


logger = logging.getLogger(__name__)


class SchedulerWorker:
    """复用现有调度服务的单任务 Redis 消费 Worker。"""

    def __init__(self, worker_id: str | None = None) -> None:
        """
        初始化调度 Worker 及其 Redis 队列客户端。

        :param worker_id: Worker 标识，不传时自动生成一个短标识。
        """
        self.redis = get_redis()
        self.queue = TaskQueue(self.redis)
        self.worker_id = worker_id or f"scheduler-{uuid.uuid4().hex[:8]}"
        self._last_reconcile_at = 0.0
        self._last_assigned_reconcile_at = 0.0

    async def run(self) -> None:
        """
        启动调度 Worker 主循环。

        Worker 会定期回收过期租约、提升到期重试任务、补偿遗漏的任务，
        然后从 Redis 领取任务并调用调度服务处理。
        """
        await self.redis.ping()
        logger.info("调度 Worker 已启动：%s", self.worker_id)
        stop_event = asyncio.Event()
        map_cache_task = asyncio.create_task(
            run_map_cache_listener(stop_event),
            name=f"{self.worker_id}-map-cache-listener",
        )
        try:
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
        finally:
            stop_event.set()
            map_cache_task.cancel()
            try:
                await map_cache_task
            except asyncio.CancelledError:
                pass

    async def _process(self, claim: TaskClaim) -> None:
        """
        处理 Worker 领取到的单个任务。

        :param claim: Redis 返回的任务主键和当前重试次数。
        """
        try:
            async with SessionLocal() as db:
                result = await trigger_dispatch(db, claim.task_id, None, False)
        except asyncio.CancelledError:
            raise
        except MapVersionUnavailableError as exc:
            # 地图版本冲突不是临时网络错误，直接挂起任务，避免 Worker 反复重试旧地图。
            logger.error("任务地图版本已变化，停止调度：task_id=%s, error=%s", claim.task_id, exc)
            await self._suspend_task(claim.task_id, str(exc))
            await self.queue.ack(claim.task_id)
            return
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

    async def _suspend_task(self, task_id: int, reason: str) -> None:
        """
        因地图版本失效等不可重试原因挂起任务并释放机器人资源。

        :param task_id: 需要挂起的任务主键。
        :param reason: 挂起原因。
        """
        async with SessionLocal() as db:
            task = await db.scalar(select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update())
            if not task:
                return
            if task.status in {TaskStatus.PENDING_ASSIGN, TaskStatus.ASSIGNED, TaskStatus.SUSPENDED}:
                variables = from_json_text(task.variables, {})
                if task.agv_id:
                    await mark_robot_idle(db, task.agv_id, variables.get("currentSite"))
                    await release_reserved_map_nodes(db, task, task.agv_id)
                task.status = TaskStatus.SUSPENDED
                task.ended_reason = reason[:500]
                create_task_log(db, task.id, task.ended_reason, level="ERROR")
                await db.commit()

    async def _suspend_after_retries(self, task_id: int, error: str) -> None:
        """
        在任务达到最大重试次数后将其标记为挂起。

        :param task_id: 重试耗尽的任务主键。
        :param error: 最后一次调度失败原因。
        """
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
        """
        定期扫描待分配任务并补偿写入 Redis 待调度队列。

        该方法用于处理任务已经写入数据库但 Redis 入队失败或消息丢失的情况。
        """
        now = time.monotonic()
        if now - self._last_reconcile_at < settings.scheduler_reconcile_interval_seconds:   #距离上次补偿扫描还没有达到规定间隔，就先不扫描
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
        """
        定期扫描已分配但尚未成功投递 RMF 的任务并重新触发投递。

        该补偿逻辑只处理尚未记录 ``rmfPublished`` 的任务。
        """
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
                logger.exception("重新向 RabbitMQ 投递已分配任务失败：task_id=%s", task_id)


async def run_scheduler() -> None:
    """以独立进程方式运行调度 Worker。"""
    try:
        await SchedulerWorker().run()
    finally:
        await close_redis()
