from __future__ import annotations

from datetime import timedelta
import logging
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import create_block_plan, create_task_log, mark_robot_busy
from common.enums.block_status import BlockStatus
from common.enums.task_status import TaskStatus
from common.exception.base import ResourceUnavailableError, StatusNotAllowedError, TaskNotFoundError
from common.utils import from_json_text, now, to_json_text
from core.conf import settings
from database.models import RobotCurrentState, RobotItem, WindTaskRecord, WorkSite
from plugin.ortools.solver import OrtoolsSolver
from scheduler.rabbitmq import publish_rmf_dispatch


logger = logging.getLogger(__name__)
solver = OrtoolsSolver()


async def trigger_dispatch(db: AsyncSession, task_id: int | None, agv_id: str | None, force: bool) -> dict:
    """为任务分配机器人、持久化分配结果，并写入 RMF 调度队列。"""
    task = await _pick_task(db, task_id)
    if task.status not in {TaskStatus.PENDING_ASSIGN, TaskStatus.ASSIGNED, TaskStatus.SUSPENDED} and not force:
        raise StatusNotAllowedError("当前任务状态不允许调度")

    variables = from_json_text(task.variables, {})
    reuse_assignment = task.status == TaskStatus.ASSIGNED and bool(task.agv_id) and not force

    if reuse_assignment:
        selected_uuid = task.agv_id
        blocks = await create_block_plan(db, task)
        dispatch_key = variables.get("rmfDispatchKey") or _new_dispatch_key(task.id)
    else:
        candidates = await _robot_candidates(db)
        selected = solver.choose_robot(candidates, agv_id or task.agv_id)
        if not selected:
            raise ResourceUnavailableError("当前没有可用于调度的机器人")

        selected_uuid = selected["uuid"]
        task.agv_id = selected_uuid
        task.status = TaskStatus.ASSIGNED
        blocks = await create_block_plan(db, task)
        dispatch_key = _new_dispatch_key(task.id)
        select_block = next((block for block in blocks if block.block_name == "CSelectAgvBp"), None)
        if select_block:
            select_block.status = BlockStatus.SUCCESS
            select_block.started_on = select_block.started_on or now()
            select_block.ended_on = now()
            select_block.output_params = to_json_text({"selectedAgvId": selected_uuid})
        for block in blocks:
            if block.block_name == "CAgvOperationBp" and not block.order_id:
                block.order_id = f"RMF-{task.id}-{block.block_id.split('-')[-1]}"

        await _reserve_sites(db, task, selected_uuid)
        await mark_robot_busy(db, selected_uuid, task.id, variables.get("currentSite"))
        create_task_log(db, task.id, f"任务已分配给机器人 {selected_uuid}")

    variables["selectedAgvId"] = selected_uuid
    variables["rmfDispatchKey"] = dispatch_key
    variables["rmfPublished"] = False
    task.variables = to_json_text(variables)

    # 先持久化任务分配结果，再向 RabbitMQ 投递消息。
    await db.commit()
    rabbit_status = await _publish_assignment_message(
        db,
        task,
        selected_uuid,
        len(blocks),
        dispatch_key,
    )
    return {
        "taskId": str(task.id),
        "status": task.status,
        "agvId": selected_uuid,
        "blockCount": len(blocks),
        "rabbitStatus": rabbit_status,
    }


def _new_dispatch_key(task_id: int) -> str:
    return f"task-{task_id}-{uuid4().hex}"


async def _publish_assignment_message(
    db: AsyncSession,
    task: WindTaskRecord,
    agv_id: str,
    block_count: int,
    dispatch_key: str,
) -> str:
    payload = {
        "event": "task.assigned",
        "taskId": str(task.id),
        "agvId": agv_id,
        "blockCount": block_count,
        "dispatchKey": dispatch_key,
    }
    try:
        await publish_rmf_dispatch(payload)
    except Exception:
        logger.exception("任务分配结果已保存，但 RabbitMQ 投递失败：task_id=%s", task.id)
        return "PENDING_RABBITMQ"

    variables = from_json_text(task.variables, {})
    variables["rmfPublished"] = True
    task.variables = to_json_text(variables)
    await db.commit()
    return "PUBLISHED"


async def _pick_task(db: AsyncSession, task_id: int | None) -> WindTaskRecord:
    if task_id is not None:
        task = await db.scalar(
            select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update()
        )
        if not task:
            raise TaskNotFoundError()
        return task
    task = await db.scalar(
        select(WindTaskRecord)
        .where(WindTaskRecord.status == TaskStatus.PENDING_ASSIGN)
        .order_by(WindTaskRecord.priority.desc(), WindTaskRecord.created_on.asc())
        .with_for_update()
    )
    if not task:
        raise ResourceUnavailableError("当前没有待调度任务")
    return task


async def _robot_candidates(db: AsyncSession) -> list[dict]:
    robots = (await db.scalars(select(RobotItem).where(RobotItem.del_ == 0))).all()
    states = {row.uuid: row for row in (await db.scalars(select(RobotCurrentState))).all()}
    heartbeat_cutoff = now() - timedelta(seconds=settings.robot_heartbeat_timeout_seconds)
    payload = []
    for robot in robots:
        state = states.get(robot.uuid)
        if not state or state.last_heartbeat_at is None or state.last_heartbeat_at < heartbeat_cutoff:
            continue
        merged = robot.to_dict()
        merged.update(state.to_dict())
        payload.append(merged)
    return payload


async def _reserve_sites(db: AsyncSession, task: WindTaskRecord, agv_id: str) -> None:
    input_params = from_json_text(task.input_params, {})
    from_site = await db.scalar(select(WorkSite).where(WorkSite.site_id == input_params.get("from")))
    to_site = await db.scalar(select(WorkSite).where(WorkSite.site_id == input_params.get("to")))
    if from_site:
        from_site.preparing = 1
        from_site.agv_id = agv_id
        from_site.holder = 3
    if to_site:
        to_site.preparing = 1
        to_site.agv_id = agv_id
        to_site.holder = 3
