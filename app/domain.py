from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common.enums.block_status import BlockStatus
from common.enums.dispatch_status import DispatchStatus
from common.enums.log_level import LogLevel
from common.enums.robot_status import RobotStatus
from common.enums.task_status import TaskStatus
from common.utils import format_dt, from_json_text, normalize_site_path, now, to_json_text
from database.models import AlarmRecord, RobotCurrentState, RobotStatusRecord, WindBlockRecord, WindTaskLog, WindTaskRecord

TASK_STATUS_DESC = {
    TaskStatus.CREATED: "待创建",
    TaskStatus.PENDING_ASSIGN: "待分配",
    TaskStatus.ASSIGNED: "已分配",
    TaskStatus.DISPATCHING: "下发中",
    TaskStatus.DISPATCHED: "已下发",
    TaskStatus.EXECUTING: "执行中",
    TaskStatus.COMPLETED: "已完成",
    TaskStatus.CANCELLED: "已取消",
    TaskStatus.FAILED: "执行失败",
    TaskStatus.SUSPENDED: "挂起/异常待处理",
}

BLOCK_STATUS_DESC = {
    BlockStatus.CREATED: "待执行",
    BlockStatus.RUNNING: "执行中",
    BlockStatus.SUCCESS: "已完成",
    BlockStatus.FAILED: "失败",
    BlockStatus.CANCELLED: "已取消",
}


def create_task_log(
    session: AsyncSession,
    task_record_id: int,
    message: str,
    level: str = LogLevel.INFO,
    task_block_id: int | None = None,
    task_id: int | None = None,
) -> WindTaskLog:
    """
    创建一条任务日志对象并加入当前数据库会话，记录任务执行过程中的关键事件。
    """
    # 任务日志只记录事件，不参与业务判断。
    log = WindTaskLog(
        task_record_id=task_record_id,
        task_id=task_id or task_record_id,
        task_block_id=task_block_id,
        level=str(level),
        message=message,
    )
    session.add(log)
    return log


def serialize_block(block: WindBlockRecord) -> dict[str, Any]:
    """
    将流程块 ORM 对象转换成接口可以直接返回的字典结构。
    """
    payload = block.to_dict()
    payload["blockInputParamsValue"] = from_json_text(block.block_input_params_value, {})
    payload["internalVariables"] = from_json_text(block.internal_variables, {})
    payload["outputParams"] = from_json_text(block.output_params, {})
    payload["statusDesc"] = BLOCK_STATUS_DESC.get(BlockStatus(block.status), "未知")
    return payload


def serialize_task(
    task: WindTaskRecord,
    blocks: list[WindBlockRecord] | None = None,
    logs: list[WindTaskLog] | None = None,
) -> dict[str, Any]:
    """
    将任务、流程块和任务日志统一组装成任务详情接口需要的响应结构。
    """
    # 把任务实体展开成接口友好的结构。
    input_params = from_json_text(task.input_params, {})
    path = from_json_text(task.path, {})
    variables = from_json_text(task.variables, {})
    return {
        "taskId": str(task.id),
        "status": task.status,
        "statusDesc": TASK_STATUS_DESC.get(TaskStatus(task.status), "未知"),
        "defId": str(task.def_id) if task.def_id else None,
        "defLabel": task.def_label,
        "defVersion": task.def_version,
        "fromSite": input_params.get("from"),
        "toSite": input_params.get("to"),
        "sitePath": input_params.get("sitePath") or [],
        "mapVersionId": str(task.map_version_id) if task.map_version_id else None,
        "agvId": task.agv_id,
        "priority": task.priority,
        "outOrderNo": task.out_order_no,
        "periodicTask": task.periodic_task,
        "createdOn": format_dt(task.created_on),
        "endedOn": format_dt(task.ended_on),
        "endedReason": task.ended_reason,
        "inputParams": input_params,
        "path": path,
        "variables": variables,
        "blocks": [serialize_block(item) for item in (blocks or [])],
        "logs": [item.to_dict() for item in (logs or [])],
    }


def task_requested_sites(task: WindTaskRecord) -> list[str]:
    """从任务快照中读取按顺序排列的 WMS 目标地图节点。

    :param task: WindTaskRecord
    """
    params = from_json_text(task.input_params, {})
    return normalize_site_path(params.get("sitePath") or [])


async def create_initial_block_plan(session: AsyncSession, task: WindTaskRecord) -> list[WindBlockRecord]:
    """
    在任务提交阶段创建选车流程块和第一个占位 RootBp。

    第一个 RootBp 只有在选定机器人后才能确定 ``from`` 地图节点，因此提交阶段先留空，
    后续由调度服务补全。
    """
    existing = (
        await session.scalars(
            select(WindBlockRecord).where(WindBlockRecord.task_record_id == task.id).order_by(WindBlockRecord.id.asc())
        )
    ).all()
    if existing:
        return existing

    requested_sites = task_requested_sites(task)
    if not requested_sites:
        return []
    blocks = [WindBlockRecord(
        task_record_id=task.id,
        block_id=f"{task.id}-select-agv",
        block_name="CSelectAgvBp",
        status=BlockStatus.CREATED,
        block_input_params_value=to_json_text({"keyRoute": requested_sites[0], "vehicle": task.agv_id or ""}),
        output_params=to_json_text({"selectedAgvId": task.agv_id or ""}),
        internal_variables=to_json_text({"selectedAgvId": ""}),
    ), _build_root_block(task, root_step_index=1, target_index=0, from_site=None, to_site=requested_sites[0])]
    session.add_all(blocks)
    return blocks


def _build_root_block(
    task: WindTaskRecord,
    root_step_index: int,
    target_index: int,
    from_site: str | None,
    to_site: str,
) -> WindBlockRecord:
    """为一个目标阶段创建 RootBp，动作子步骤等消息消费后再创建。"""
    root_block_id = f"{task.id}-root-{root_step_index}"
    common_variables = {
        "rootStepIndex": root_step_index,
        "targetIndex": target_index,
        "selectedAgvId": task.agv_id or "",
    }
    return WindBlockRecord(
        task_record_id=task.id,
        block_id=root_block_id,
        block_name="RootBp",
        status=BlockStatus.CREATED,
        block_input_params_value=to_json_text(
            {"from": from_site, "to": to_site}
        ),
        internal_variables=to_json_text(
            {**common_variables, "pendingStart": from_site is None}
        ),
        output_params=to_json_text({}),
    )


def build_operation_plans(
    task: WindTaskRecord,
    root: WindBlockRecord,
    segments: list[dict[str, Any]],
    script_name: str | None = None,
) -> list[dict[str, Any]]:
    """把当前 RootBp 的地图路径拆成可投递给 RabbitMQ 的动作子步骤描述。"""
    root_variables = from_json_text(root.internal_variables, {})
    root_step_index = int(root_variables.get("rootStepIndex") or 0)
    target_index = int(root_variables.get("targetIndex") or 0)
    selected_agv_id = str(root_variables.get("selectedAgvId") or task.agv_id or "")
    valid_segments = [
        segment
        for segment in segments
        if segment.get("from") and segment.get("to")
    ]
    plans: list[dict[str, Any]] = []
    for step_index, segment in enumerate(valid_segments, start=1):
        from_site = segment.get("from")
        to_site = segment.get("to")
        plans.append(
            {
                "blockId": f"{root.block_id}-op-{step_index}",
                "parentBlockId": root.block_id,
                "blockName": "CAgvOperationBp",
                "orderId": f"RMF-{task.id}-{root_step_index}-{step_index}",
                "inputParams": {
                    "from": from_site,
                    "to": to_site,
                    "scriptName": script_name if step_index == len(valid_segments) else None,
                },
                "internalVariables": {
                    "rootStepIndex": root_step_index,
                    "targetIndex": target_index,
                    "selectedAgvId": selected_agv_id,
                    "stepIndex": step_index,
                },
            }
        )
    return plans


async def create_operation_blocks(
    session: AsyncSession,
    task: WindTaskRecord,
    root: WindBlockRecord,
    plans: list[dict[str, Any]],
) -> list[WindBlockRecord]:
    """RabbitMQ 消费后幂等创建当前 RootBp 的动作子步骤。"""
    existing = (
        await session.scalars(
            select(WindBlockRecord).where(
                WindBlockRecord.task_record_id == task.id,
                WindBlockRecord.parent_block_id == root.block_id,
                WindBlockRecord.block_name == "CAgvOperationBp",
            )
        )
    ).all()
    existing_by_id = {block.block_id: block for block in existing}
    blocks: list[WindBlockRecord] = []
    for plan in plans:
        block_id = str(plan["blockId"])
        block = existing_by_id.get(block_id)
        if block is None:
            block = WindBlockRecord(
                task_record_id=task.id,
                parent_block_id=root.block_id,
                block_id=block_id,
                block_name=str(plan.get("blockName") or "CAgvOperationBp"),
                order_id=str(plan.get("orderId") or ""),
                status=BlockStatus.CREATED,
                block_input_params_value=to_json_text(plan.get("inputParams") or {}),
                internal_variables=to_json_text(plan.get("internalVariables") or {}),
                output_params=to_json_text({}),
            )
            session.add(block)
        blocks.append(block)
    await session.flush()
    return blocks


async def create_next_root_block(
    session: AsyncSession,
    task: WindTaskRecord,
    from_site: str,
    target_index: int,
) -> list[WindBlockRecord]:
    """只有当前 RootBp 完成后，才创建下一个 RootBp。"""
    requested_sites = task_requested_sites(task)
    if target_index >= len(requested_sites):
        return []
    existing_roots = (
        await session.scalars(
            select(WindBlockRecord).where(
                WindBlockRecord.task_record_id == task.id,
                WindBlockRecord.block_name == "RootBp",
            )
        )
    ).all()
    root_step_index = max(
        [
            int(from_json_text(root.internal_variables, {}).get("rootStepIndex") or 0)
            for root in existing_roots
        ],
        default=0,
    ) + 1
    root_block_id = f"{task.id}-root-{root_step_index}"
    existing = await session.scalar(
        select(WindBlockRecord).where(
            WindBlockRecord.task_record_id == task.id,
            WindBlockRecord.block_id == root_block_id,
        )
    )
    if existing:
        return [existing]
    block = _build_root_block(
        task,
        root_step_index=root_step_index,
        target_index=target_index,
        from_site=from_site,
        to_site=requested_sites[target_index],
    )
    session.add(block)
    return [block]


async def mark_robot_busy(session: AsyncSession, uuid: str, task_id: int, current_site_id: str | None = None) -> None:
    """
    将指定机器人更新为忙碌状态，并记录机器人状态变化流水。
    """
    # 任务下发后，把机器人快照切到忙碌态，并补一条状态流水。
    state = await session.scalar(select(RobotCurrentState).where(RobotCurrentState.uuid == uuid))
    if not state:
        return
    state.current_status = RobotStatus.BUSY
    state.dispatch_status = DispatchStatus.BUSY
    state.current_task_id = task_id
    if current_site_id:
        state.current_site_id = current_site_id
    state.updated_at = now()
    session.add(
        RobotStatusRecord(
            uuid=uuid,
            vehicle_name=state.vehicle_name,
            old_status=RobotStatus.IDLE,
            new_status=RobotStatus.BUSY,
            location=state.current_location or current_site_id,
        )
    )


async def mark_robot_idle(session: AsyncSession, uuid: str, current_site_id: str | None = None) -> None:
    """
    将指定机器人恢复为空闲可调度状态，并记录机器人状态变化流水。
    """
    # 任务结束或取消时，把机器人状态收回到可调度态。
    state = await session.scalar(select(RobotCurrentState).where(RobotCurrentState.uuid == uuid))
    if not state:
        return
    previous = state.current_status
    state.current_status = RobotStatus.IDLE
    state.dispatch_status = DispatchStatus.IDLE
    state.current_task_id = None
    if current_site_id:
        state.current_site_id = current_site_id
    state.updated_at = now()
    session.add(
        RobotStatusRecord(
            uuid=uuid,
            vehicle_name=state.vehicle_name,
            old_status=previous,
            new_status=RobotStatus.IDLE,
            location=state.current_location or current_site_id,
        )
    )


async def refresh_alarm_snapshot(session: AsyncSession, vehicle_id: str) -> None:
    """
    重新统计机器人的未恢复报警，并刷新机器人当前报警状态快照。
    """
    # 报警是否未恢复，直接回写到当前状态快照里供调度器过滤。
    state = await session.scalar(select(RobotCurrentState).where(RobotCurrentState.uuid == vehicle_id))
    if not state:
        return
    open_alarms = (
        await session.scalars(
            select(AlarmRecord).where(AlarmRecord.vehicle_id == vehicle_id, AlarmRecord.ended_on.is_(None)).order_by(AlarmRecord.started_on.desc())
        )
    ).all()
    state.has_unresolved_alarm = 1 if open_alarms else 0
    state.alarm_level = open_alarms[0].level if open_alarms else None
    state.updated_at = now()


async def update_task_progress(session: AsyncSession, task: WindTaskRecord) -> None:
    """
    根据已完成的 RootBp 计算任务当前进度，并回写任务变量。
    """
    roots = (
        await session.scalars(
            select(WindBlockRecord)
            .where(WindBlockRecord.task_record_id == task.id, WindBlockRecord.block_name == "RootBp")
            .order_by(WindBlockRecord.id.asc())
        )
    ).all()
    completed_roots = [root for root in roots if root.status == BlockStatus.SUCCESS]
    completed = len(completed_roots)
    requested_sites = task_requested_sites(task)
    variables = from_json_text(task.variables, {})
    variables["currentStepIndex"] = completed
    variables["totalSteps"] = len(requested_sites)
    if completed_roots:
        last_root = completed_roots[-1]
        last_root_values = from_json_text(last_root.output_params, {})
        last_root_variables = from_json_text(last_root.internal_variables, {})
        last_target_index = int(last_root_variables.get("targetIndex") or 0)
        variables["currentStepIndex"] = min(last_target_index + 1, len(requested_sites))
        variables["currentSite"] = (
            last_root_values.get("currentSite")
            or requested_sites[min(last_target_index, len(requested_sites) - 1)]
        )
        next_index = last_target_index + 1
        variables["nextSite"] = requested_sites[next_index] if next_index < len(requested_sites) else None
    elif requested_sites:
        variables["nextSite"] = requested_sites[0]
    task.variables = to_json_text(variables)
