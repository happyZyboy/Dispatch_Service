from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common.enums.block_status import BlockStatus
from common.enums.dispatch_status import DispatchStatus
from common.enums.log_level import LogLevel
from common.enums.robot_status import RobotStatus
from common.enums.task_status import TaskStatus
from common.utils import format_dt, from_json_text, now, to_json_text
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
    payload["blockInputParams"] = from_json_text(block.block_input_params, {})
    payload["blockInputParamsValue"] = from_json_text(block.block_input_params_value, {})
    payload["blockInternalVariables"] = from_json_text(block.block_internal_variables, {})
    payload["inputParams"] = from_json_text(block.input_params, {})
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


async def create_block_plan(session: AsyncSession, task: WindTaskRecord) -> list[WindBlockRecord]:
    """
    根据任务路径创建 Root、选车和分段动作流程块，供调度下发和 RMF 回调使用。
    """
    # 把整单路径拆成 Root / 选车 / 多段动作块，方便逐段回调。
    existing = (
        await session.scalars(
            select(WindBlockRecord).where(WindBlockRecord.task_record_id == task.id).order_by(WindBlockRecord.id.asc())
        )
    ).all()
    if existing:
        return existing

    path = from_json_text(task.path, {})
    variables = from_json_text(task.variables, {})
    segments = path.get("segments", [])
    route = path.get("route", [])
    blocks: list[WindBlockRecord] = [
        WindBlockRecord(
            task_id=task.id,
            task_record_id=task.id,
            block_id=f"{task.id}-root",
            block_name="RootBp",
            status=BlockStatus.SUCCESS,
            started_on=task.created_on,
            ended_on=task.created_on,
            block_input_params_value=to_json_text({"from": route[0] if route else None, "to": route[-1] if route else None}),
            internal_variables=to_json_text({"taskRecordId": task.id, "stepIndex": 0, "totalSteps": len(segments)}),
            output_params=to_json_text({}),
        ),
        WindBlockRecord(
            task_id=task.id,
            task_record_id=task.id,
            block_id=f"{task.id}-select-agv",
            block_name="CSelectAgvBp",
            status=BlockStatus.SUCCESS if task.agv_id else BlockStatus.CREATED,
            started_on=task.created_on if task.agv_id else None,
            ended_on=task.created_on if task.agv_id else None,
            block_input_params=to_json_text({"vehicle": {"type": "Expression", "value": "taskInputs.vehicle"}}),
            block_input_params_value=to_json_text({"vehicle": task.agv_id or ""}),
            output_params=to_json_text({"selectedAgvId": task.agv_id or ""}),
            internal_variables=to_json_text({"taskRecordId": task.id, "stepIndex": 0, "totalSteps": len(segments)}),
        ),
    ]
    for segment in segments:
        step_index = segment["stepIndex"]
        blocks.append(
            WindBlockRecord(
                task_id=task.id,
                task_record_id=task.id,
                block_id=f"{task.id}-move-{step_index}",
                block_name="CAgvOperationBp",
                status=BlockStatus.CREATED,
                block_input_params=to_json_text(
                    {
                        "from": {"type": "Simple", "value": segment["from"], "required": True},
                        "to": {"type": "Simple", "value": segment["to"], "required": True},
                        "scriptName": {"type": "Simple", "value": "binTask"},
                        "var_param": {"type": "Simple", "value": "move", "required": True},
                    }
                ),
                block_input_params_value=to_json_text(
                    {
                        "stepIndex": step_index,
                        "from": segment["from"],
                        "to": segment["to"],
                        "scriptName": "binTask",
                        "var_param": "move",
                    }
                ),
                internal_variables=to_json_text(
                    {
                        "taskRecordId": task.id,
                        "stepIndex": step_index,
                        "totalSteps": len(segments),
                        "selectedAgvId": task.agv_id or variables.get("selectedAgvId", ""),
                    }
                ),
                input_params=task.input_params,
                output_params=to_json_text({}),
            )
        )
    session.add_all(blocks)
    return blocks


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
    根据已完成的动作流程块计算任务当前进度，并回写任务变量。
    """
    # 只按已完成的动作块回推整单进度，避免把中间态算进去。
    blocks = (
        await session.scalars(
            select(WindBlockRecord).where(WindBlockRecord.task_record_id == task.id, WindBlockRecord.block_name == "CAgvOperationBp").order_by(WindBlockRecord.id.asc())
        )
    ).all()
    completed = sum(1 for block in blocks if block.status == BlockStatus.SUCCESS)
    variables = from_json_text(task.variables, {})
    path = from_json_text(task.path, {})
    route = path.get("route", [])
    variables["currentStepIndex"] = completed
    variables["totalSteps"] = len(blocks)
    if completed < len(route):
        variables["currentSite"] = route[completed]
    if completed + 1 < len(route):
        variables["nextSite"] = route[completed + 1]
    task.variables = to_json_text(variables)
