from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from sqlalchemy import select

from app.domain import build_operation_plans, create_operation_blocks, create_task_log, mark_robot_idle
from common.enums.block_status import BlockStatus
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
    """消息已经不再代表当前任务分配结果。"""


class RmfDispatchError(Exception):
    """下游 RMF 调用失败，可以继续重试。"""


class RmfDispatchConsumer:
    def __init__(self) -> None:
        self.rabbitmq = get_rabbitmq()

    async def run(self) -> None:
        queue = await self.rabbitmq.get_queue()
        logger.info("RMF 调度消费者已启动：队列=%s", settings.rabbitmq_queue)
        async with queue.iterator() as messages:
            async for message in messages:
                await self._handle_message(message)

    async def _handle_message(self, message: Any) -> None:
        payload: dict[str, Any] | None = None
        try:
            payload = json.loads(message.body.decode("utf-8"))
            await self._dispatch(payload)
        except (ValueError, KeyError, StaleDispatchMessage) as exc:
            logger.warning("丢弃过期或无效的 RMF 消息：%s", exc)
            await message.reject(requeue=False)
            return
        except Exception as exc:
            attempt = self._message_attempt(message)
            if payload is not None and attempt < settings.rabbitmq_max_retries:
                try:
                    await publish_rmf_dispatch(payload, attempt=attempt + 1)
                    await message.ack()
                    logger.warning(
                        "RMF 下发失败，消息已安排重试：task_id=%s attempt=%s",
                        payload.get("taskId"),
                        attempt + 1,
                    )
                    return
                except Exception:
                    logger.exception("重新投递 RMF 消息失败")
                    await message.nack(requeue=True)
                    return

            if payload is not None:
                await self._mark_failed(payload, str(exc))
            logger.exception("RMF 下发重试次数已耗尽：task_id=%s", (payload or {}).get("taskId"))
            await message.reject(requeue=False)
            return

        await message.ack()

    async def _dispatch(self, payload: dict[str, Any]) -> None:
        task_id = int(payload["taskId"])
        agv_id = str(payload["agvId"])
        dispatch_key = str(payload["dispatchKey"])
        root_block_id = str(payload.get("rootBlockId") or "")
        claim = await self._claim_task(
            task_id,
            agv_id,
            dispatch_key,
            root_block_id,
            payload.get("operations") or [],
            payload.get("segments") or [],
        )
        if claim is None:
            return

        try:
            result = await asyncio.to_thread(rmf_client.submit_block, claim)
            if not result or result.get("success") is False:
                raise RmfDispatchError(f"RMF 拒绝任务：{task_id}")
        except Exception as exc:
            await self._reset_for_retry(task_id, str(exc), claim.get("rootBlockId"))
            raise

        await self._mark_dispatched(task_id, dispatch_key, result)

    async def _claim_task(
        self,
        task_id: int,
        agv_id: str,
        dispatch_key: str,
        requested_root_id: str,
        planned_operations: list[dict[str, Any]],
        planned_segments: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        async with SessionLocal() as db:
            task = await db.scalar(
                select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update()
            )
            if not task:
                raise StaleDispatchMessage(f"任务不存在：{task_id}")
            if task.status == TaskStatus.DISPATCHED:
                return None
            if task.status not in {TaskStatus.ASSIGNED, TaskStatus.DISPATCHING}:
                raise StaleDispatchMessage(f"任务状态不可下发：{task.status}")
            if task.agv_id != agv_id:
                raise StaleDispatchMessage(f"机器人分配已变更：task_id={task_id}")

            variables = from_json_text(task.variables, {})
            if variables.get("rmfDispatchKey") != dispatch_key:
                raise StaleDispatchMessage(f"调度键已变更：task_id={task_id}")
            if task.status == TaskStatus.DISPATCHING:
                started_at = float(variables.get("rmfDispatchStartedAt") or 0)
                if time.time() - started_at < settings.rabbitmq_dispatch_lease_seconds:
                    return None

            # 当前有效的 RootBp 是任务中唯一需要发送给 RMF 的部分。
            root = await db.scalar(
                select(WindBlockRecord)
                .where(
                    WindBlockRecord.task_record_id == task.id,
                    WindBlockRecord.block_name == "RootBp",
                    WindBlockRecord.status.in_([BlockStatus.CREATED, BlockStatus.RUNNING]),
                )
                .order_by(WindBlockRecord.id.desc())
            )
            if not root:
                raise StaleDispatchMessage(f"没有可用的 RootBp：task_id={task_id}")
            if requested_root_id and root.block_id != requested_root_id:
                raise StaleDispatchMessage(f"RootBp 已变更：task_id={task_id}")
            planned_operations = planned_operations or build_operation_plans(task, root, planned_segments)
            children = await create_operation_blocks(db, task, root, planned_operations)
            if not children:
                raise StaleDispatchMessage(f"RootBp 没有动作流程块：{root.block_id}")
            root.status = BlockStatus.RUNNING
            root.started_on = root.started_on or now()
            active_root_id = root.block_id

            task.status = TaskStatus.DISPATCHING
            variables["rmfDispatchStartedAt"] = time.time()
            task.variables = to_json_text(variables)
            create_task_log(db, task.id, "已领取 RMF 下发任务")
            await db.commit()
            return {
                "taskId": task.id,
                "agvId": agv_id,
                "dispatchKey": dispatch_key,
                "rootBlockId": active_root_id,
                "rootStepIndex": from_json_text(root.internal_variables, {}).get("rootStepIndex"),
                "blocks": [
                    {
                        "blockId": child.block_id,
                        "orderId": child.order_id,
                        "inputParams": from_json_text(child.block_input_params_value, {}),
                    }
                    for child in children
                ],
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
                raise StaleDispatchMessage(f"RMF 提交前调度键已变更：task_id={task_id}")
            rmf_task_id = self._extract_rmf_task_id(result)
            if rmf_task_id is not None:
                variables["rmfTaskId"] = rmf_task_id
            variables.pop("rmfDispatchStartedAt", None)
            variables["rmfPublished"] = True
            task.variables = to_json_text(variables)
            task.status = TaskStatus.DISPATCHED
            create_task_log(db, task.id, "RMF 已接受任务下发")
            await db.commit()

    async def _reset_for_retry(self, task_id: int, error: str, root_block_id: str | None) -> None:
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
            if root_block_id:
                root = await db.scalar(
                    select(WindBlockRecord).where(
                        WindBlockRecord.task_record_id == task.id,
                        WindBlockRecord.block_id == root_block_id,
                    )
                )
                if root and root.status == BlockStatus.RUNNING:
                    root.status = BlockStatus.CREATED
                    root.started_on = None
            task.ended_reason = error[:500]
            create_task_log(db, task.id, f"RMF 下发重试：{error[:500]}")
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
            task.ended_reason = f"RMF 下发重试次数已耗尽：{error[:500]}"
            root_block_id = str(payload.get("rootBlockId") or "")
            if root_block_id:
                root = await db.scalar(
                    select(WindBlockRecord).where(
                        WindBlockRecord.task_record_id == task.id,
                        WindBlockRecord.block_id == root_block_id,
                    )
                )
                if root:
                    root.status = BlockStatus.FAILED
                    root.ended_on = now()
                    root.ended_reason = error[:500]
            if task.agv_id:
                await mark_robot_idle(db, task.agv_id, current_site)
                await self._release_sites(db, task, task.agv_id)
            create_task_log(db, task.id, task.ended_reason, level="ERROR")
            await db.commit()

    async def _release_sites(self, db: Any, task: WindTaskRecord, agv_id: str) -> None:
        input_params = from_json_text(task.input_params, {})
        path = from_json_text(task.path, {})
        site_ids = set(path.get("route") or input_params.get("sitePath") or []) - {None}
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
