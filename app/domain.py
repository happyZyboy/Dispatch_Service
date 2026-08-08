from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common.enums.block_status import BlockStatus
from common.enums.dispatch_status import DispatchStatus
from common.enums.log_level import LogLevel
from common.enums.robot_status import RobotStatus
from common.enums.task_status import TaskStatus
from common.utils import (
    format_dt,
    from_json_text,
    normalize_site_path,
    now,
    robot_location_json,
    to_json_text,
)
from database.models import AlarmRecord, MapNode, RobotCurrentState, RobotStatusRecord, WindBlockRecord, WindTaskLog, WindTaskRecord

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


async def create_root_block(
    session: AsyncSession,
    task: WindTaskRecord,
    root_step_index: int,
    target_index: int,
    from_site: str | None,
    to_site: str,
) -> WindBlockRecord:
    """
    在调度阶段幂等创建一个 RootBp 流程块。

    第一个 RootBp 创建时可以把 ``from`` 留为空，表示机器人真实起点还需要
    通过选车和地图接入计算确定；后续 RootBp 则直接使用上一阶段的目标节点作为起点。

    :param session: 当前异步数据库会话。
    :param task: 当前任务记录。
    :param root_step_index: RootBp 阶段序号，从 1 开始。
    :param target_index: 当前目标在 WMS sitePath 中的下标。
    :param from_site: 当前阶段的地图起点，可以为空。
    :param to_site: 当前阶段的目标地图节点。
    :return: 已存在或新创建的 RootBp 记录。
    """
    root_block_id = f"{task.id}-root-{root_step_index}"
    existing = await session.scalar(
        select(WindBlockRecord).where(
            WindBlockRecord.task_record_id == task.id,
            WindBlockRecord.block_id == root_block_id,
            WindBlockRecord.block_name == "RootBp",
        )
    )
    if existing:
        return existing

    root = _build_root_block(
        task,
        root_step_index=root_step_index,
        target_index=target_index,
        from_site=from_site,
        to_site=to_site,
    )
    session.add(root)
    await session.flush()
    return root


async def create_select_robot_block(
    session: AsyncSession,
    task: WindTaskRecord,
    root: WindBlockRecord,
    selected_agv_id: str | None = None,
) -> WindBlockRecord:
    """
    创建或更新当前 RootBp 下的 CSelectAgvBp 选车步骤。

    机器人选定后再次调用本方法，会把选中的机器人写入输出参数和内部变量，
    并将选车步骤标记为成功；这样指定机器人和自动选车共用同一套记录逻辑。

    :param session: 当前异步数据库会话。
    :param task: 当前任务记录。
    :param root: CSelectAgvBp 所属的父 RootBp。
    :param selected_agv_id: 已选中的机器人编码，创建占位记录时可以为空。
    :return: 已存在或新创建的 CSelectAgvBp 记录。
    """
    existing = await session.scalar(
        select(WindBlockRecord).where(
            WindBlockRecord.task_record_id == task.id,
            WindBlockRecord.parent_block_id == root.block_id,
            WindBlockRecord.block_name == "CSelectAgvBp",
        )
    )
    select_block = existing
    if select_block is None:
        requested_sites = task_requested_sites(task)
        select_block = WindBlockRecord(
            task_record_id=task.id,
            parent_block_id=root.block_id,
            block_id=f"{root.block_id}-select-agv",
            block_name="CSelectAgvBp",
            status=BlockStatus.CREATED,
            block_input_params_value=to_json_text(
                {
                    "keyRoute": requested_sites[0] if requested_sites else None,
                    "vehicle": task.agv_id or "",
                }
            ),
            output_params=to_json_text({"selectedAgvId": ""}),
            internal_variables=to_json_text({"selectedAgvId": ""}),
        )
        session.add(select_block)

    if selected_agv_id:
        selected_agv_id = str(selected_agv_id)
        select_block.status = BlockStatus.SUCCESS
        select_block.started_on = select_block.started_on or now()
        select_block.ended_on = now()
        select_block.output_params = to_json_text({"selectedAgvId": selected_agv_id})
        select_block.internal_variables = to_json_text({"selectedAgvId": selected_agv_id})

    await session.flush()
    return select_block


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
            {
                "from": from_site,
                "to": to_site,
                "pendingStart": from_site is None,
            }
        ),
        internal_variables=to_json_text(common_variables),
        output_params=to_json_text({}),
    )


async def create_next_operation_block(
    session: AsyncSession,
    task: WindTaskRecord,
    root: WindBlockRecord,
    segments: list[dict[str, Any]],
) -> WindBlockRecord | None:
    """
    为当前 RootBp 按路线顺序创建下一条 CAgvOperationBp。

    当前阶段只保留一条尚未完成的移动步骤：如果已经存在未完成的步骤，
    直接返回它；如果上一条已经完成，则根据下一个地图分段创建新步骤。
    坐标接入分段允许 ``from`` 为空，表示从机器人真实坐标移动到第一个地图节点。

    :param session: 当前异步数据库会话。
    :param task: 当前任务记录。
    :param root: 当前动作所属的 RootBp。
    :param segments: 当前 RootBp 对应的连续地图分段。
    :return: 下一条已存在或新创建的动作步骤；没有剩余分段时返回 None。
    """
    operation_blocks = (
        await session.scalars(
            select(WindBlockRecord)
            .where(
                WindBlockRecord.task_record_id == task.id,
                WindBlockRecord.parent_block_id == root.block_id,
                WindBlockRecord.block_name == "CAgvOperationBp",
            )
            .order_by(WindBlockRecord.id.asc())
        )
    ).all()
    for operation_block in operation_blocks:
        if operation_block.status != BlockStatus.SUCCESS:
            return operation_block

    next_index = len(operation_blocks)
    if next_index >= len(segments):
        return None

    segment = segments[next_index]
    root_variables = from_json_text(root.internal_variables, {})
    root_step_index = int(root_variables.get("rootStepIndex") or 0)
    target_index = int(root_variables.get("targetIndex") or 0)
    selected_agv_id = str(root_variables.get("selectedAgvId") or task.agv_id or "")
    block_id = f"{root.block_id}-op-{next_index + 1}"
    existing = await session.scalar(
        select(WindBlockRecord).where(
            WindBlockRecord.task_record_id == task.id,
            WindBlockRecord.block_id == block_id,
            WindBlockRecord.block_name == "CAgvOperationBp",
        )
    )
    if existing:
        return existing

    operation_block = WindBlockRecord(
        task_record_id=task.id,
        parent_block_id=root.block_id,
        block_id=block_id,
        block_name="CAgvOperationBp",
        order_id=f"RMF-{task.id}-{root_step_index}-{next_index + 1}",
        status=BlockStatus.CREATED,
        block_input_params_value=to_json_text(
            {
                "from": segment.get("from"),
                "to": segment.get("to"),
                "scriptName": "move",
                "segmentType": segment.get("segmentType", "map"),
                "startPose": segment.get("startPose"),
            }
        ),
        internal_variables=to_json_text(
            {
                "rootStepIndex": root_step_index,
                "targetIndex": target_index,
                "selectedAgvId": selected_agv_id,
                "stepIndex": next_index + 1,
            }
        ),
        output_params=to_json_text({}),
    )
    session.add(operation_block)
    await session.flush()
    return operation_block


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
            location=robot_location_json(state.current_x, state.current_y),
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
            location=robot_location_json(state.current_x, state.current_y),
        )
    )


async def release_reserved_map_nodes(
    session: AsyncSession,
    task: WindTaskRecord,
    agv_id: str | None = None,
) -> None:
    """
    释放任务在其绑定地图版本中预占的地图节点，避免异常结束后资源一直被占用。
    """
    input_params = from_json_text(task.input_params, {})
    path = from_json_text(task.path, {})
    node_codes = set(path.get("route") or input_params.get("sitePath") or []) - {None}
    if task.map_version_id is None or not node_codes:
        return

    owner_agv_id = agv_id or task.agv_id
    nodes = (
        await session.scalars(
            select(MapNode).where(
                MapNode.map_version_id == task.map_version_id,
                MapNode.node_code.in_(node_codes),
                MapNode.del_ == 0,
            )
        )
    ).all()
    for node in nodes:
        if node.agv_id not in {None, owner_agv_id}:
            continue
        node.preparing = 0
        node.agv_id = None
        node.holder = 0


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
