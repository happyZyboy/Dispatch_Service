from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from sqlalchemy import func, select

from app.domain import create_task_log, mark_robot_idle
from common.enums.task_status import TaskStatus
from common.utils import from_json_text, now, to_json_text
from core.conf import settings
from database.db import SessionLocal
from database.models import WindBlockRecord, WindTaskRecord, WorkSite
from plugin.rmf.client import RmfClient
from scheduler.rabbitmq import close_rabbitmq, get_rabbitmq, publish_rmf_dispatch


logger = logging.getLogger(__name__)
rmf_client = RmfClient()


class StaleDispatchMessage(Exception):
    """The message no longer represents the current task assignment."""


class RmfDispatchError(Exception):
    """The downstream RMF call failed and may be retried."""


class RmfDispatchConsumer:
    def __init__(self) -> None:
        self.rabbitmq = get_rabbitmq()

    async def run(self) -> None:
        queue = await self.rabbitmq.get_queue()
        logger.info("RMF dispatch consumer started: queue=%s", settings.rabbitmq_queue)
        async with queue.iterator() as messages:
            async for message in messages:
                await self._handle_message(message)

    async def _handle_message(self, message: Any) -> None:
        payload: dict[str, Any] | None = None
        try:
            payload = json.loads(message.body.decode("utf-8"))
            await self._dispatch(payload)
        except (ValueError, KeyError, StaleDispatchMessage) as exc:
            logger.warning("discarding stale or invalid RMF message: %s", exc)
            await message.reject(requeue=False)
            return
        except Exception as exc:
            attempt = self._message_attempt(message)
            if payload is not None and attempt < settings.rabbitmq_max_retries:
                try:
                    await publish_rmf_dispatch(payload, attempt=attempt + 1)
                    await message.ack()
                    logger.warning(
                        "RMF dispatch failed, message scheduled for retry: task_id=%s attempt=%s",
                        payload.get("taskId"),
                        attempt + 1,
                    )
                    return
                except Exception:
                    logger.exception("failed to republish RMF message")
                    await message.nack(requeue=True)
                    return

            if payload is not None:
                await self._mark_failed(payload, str(exc))
            logger.exception("RMF dispatch retries exhausted: task_id=%s", (payload or {}).get("taskId"))
            await message.reject(requeue=False)
            return

        await message.ack()

    async def _dispatch(self, payload: dict[str, Any]) -> None:
        task_id = int(payload["taskId"])
        agv_id = str(payload["agvId"])
        dispatch_key = str(payload["dispatchKey"])
        claim = await self._claim_task(task_id, agv_id, dispatch_key)
        if claim is None:
            return

        try:
            result = await asyncio.to_thread(rmf_client.submit_block, claim)
            if not result or result.get("success") is False:
                raise RmfDispatchError(f"RMF rejected task {task_id}")
        except Exception as exc:
            await self._reset_for_retry(task_id, str(exc))
            raise

        await self._mark_dispatched(task_id, dispatch_key, result)

    async def _claim_task(self, task_id: int, agv_id: str, dispatch_key: str) -> dict[str, Any] | None:
        async with SessionLocal() as db:
            task = await db.scalar(
                select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update()
            )
            if not task:
                raise StaleDispatchMessage(f"task not found: {task_id}")
            if task.status == TaskStatus.DISPATCHED:
                return None
            if task.status not in {TaskStatus.ASSIGNED, TaskStatus.DISPATCHING}:
                raise StaleDispatchMessage(f"task status is not dispatchable: {task.status}")
            if task.agv_id != agv_id:
                raise StaleDispatchMessage(f"robot assignment changed: task_id={task_id}")

            variables = from_json_text(task.variables, {})
            if variables.get("rmfDispatchKey") != dispatch_key:
                raise StaleDispatchMessage(f"dispatch key changed: task_id={task_id}")
            if task.status == TaskStatus.DISPATCHING:
                started_at = float(variables.get("rmfDispatchStartedAt") or 0)
                if time.time() - started_at < settings.rabbitmq_dispatch_lease_seconds:
                    return None

            task.status = TaskStatus.DISPATCHING
            variables["rmfDispatchStartedAt"] = time.time()
            task.variables = to_json_text(variables)
            create_task_log(db, task.id, "RMF dispatch claimed")
            block_count = await db.scalar(
                select(func.count(WindBlockRecord.id)).where(WindBlockRecord.task_record_id == task.id)
            )
            await db.commit()
            return {
                "taskId": task.id,
                "agvId": agv_id,
                "dispatchKey": dispatch_key,
                "blocks": block_count or 0,
            }

    async def _mark_dispatched(self, task_id: int, dispatch_key: str, result: dict[str, Any]) -> None:
        async with SessionLocal() as db:
            task = await db.scalar(
                select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update()
            )
            if not task or task.status == TaskStatus.DISPATCHED:
                return

            variables = from_json_text(task.variables, {})
            if variables.get("rmfDispatchKey") != dispatch_key:
                raise StaleDispatchMessage(f"dispatch key changed before RMF commit: task_id={task_id}")
            rmf_task_id = self._extract_rmf_task_id(result)
            if rmf_task_id is not None:
                variables["rmfTaskId"] = rmf_task_id
            variables.pop("rmfDispatchStartedAt", None)
            variables["rmfPublished"] = True
            task.variables = to_json_text(variables)
            task.status = TaskStatus.DISPATCHED
            create_task_log(db, task.id, "RMF accepted dispatch")
            await db.commit()

    async def _reset_for_retry(self, task_id: int, error: str) -> None:
        async with SessionLocal() as db:
            task = await db.scalar(
                select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update()
            )
            if not task or task.status == TaskStatus.DISPATCHED:
                return
            variables = from_json_text(task.variables, {})
            variables.pop("rmfDispatchStartedAt", None)
            task.variables = to_json_text(variables)
            task.status = TaskStatus.ASSIGNED
            task.ended_reason = error[:500]
            create_task_log(db, task.id, f"RMF dispatch retry: {error[:500]}")
            await db.commit()

    async def _mark_failed(self, payload: dict[str, Any], error: str) -> None:
        task_id = int(payload["taskId"])
        async with SessionLocal() as db:
            task = await db.scalar(
                select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update()
            )
            if not task or task.status == TaskStatus.DISPATCHED:
                return
            variables = from_json_text(task.variables, {})
            current_site = variables.get("currentSite")
            task.status = TaskStatus.FAILED
            task.ended_on = now()
            task.ended_reason = f"RMF dispatch retries exhausted: {error[:500]}"
            if task.agv_id:
                await mark_robot_idle(db, task.agv_id, current_site)
                await self._release_sites(db, task, task.agv_id)
            create_task_log(db, task.id, task.ended_reason, level="ERROR")
            await db.commit()

    async def _release_sites(self, db: Any, task: WindTaskRecord, agv_id: str) -> None:
        input_params = from_json_text(task.input_params, {})
        site_ids = {input_params.get("from"), input_params.get("to")} - {None}
        if not site_ids:
            return
        sites = (
            await db.scalars(select(WorkSite).where(WorkSite.site_id.in_(site_ids)))
        ).all()
        for site in sites:
            if site.agv_id == agv_id:
                site.preparing = 0
                site.agv_id = None
                site.holder = 0

    @staticmethod
    def _message_attempt(message: Any) -> int:
        headers = message.headers or {}
        return int(headers.get("x-attempt", 0))

    @staticmethod
    def _extract_rmf_task_id(result: dict[str, Any]) -> Any:
        return (
            result.get("rmfTaskId")
            or result.get("taskId")
            or (result.get("payload") or {}).get("rmfTaskId")
        )


async def run_rmf_consumer() -> None:
    try:
        await RmfDispatchConsumer().run()
    finally:
        await close_rabbitmq()
