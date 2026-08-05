from __future__ import annotations

from datetime import timedelta
import logging
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import build_operation_plans, create_task_log, mark_robot_busy, task_requested_sites
from common.enums.block_status import BlockStatus
from common.enums.task_status import TaskStatus
from common.exception.base import ResourceUnavailableError, StatusNotAllowedError, TaskNotFoundError
from common.utils import build_route, from_json_text, now, to_json_text
from core.conf import settings
from database.models import RobotCurrentState, RobotItem, WindBlockRecord, WindTaskRecord, WorkSite
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
    requested_sites = task_requested_sites(task)
    if not requested_sites:
        raise ResourceUnavailableError("任务没有可执行的目标库位")

    if reuse_assignment:
        selected_uuid = str(task.agv_id)
        root = await _active_root(db, task)
        if root is None:
            raise ResourceUnavailableError("任务没有待下发的 RootBp")
        dispatch_key = variables.get("rmfDispatchKey") or _new_dispatch_key(task.id)
    else:
        candidates = await _robot_candidates(db)
        site_positions = await _site_positions(db)
        selected = solver.choose_robot(
            candidates,
            agv_id or task.agv_id,
            target_site=requested_sites[0],
            site_positions=site_positions,
        )
        if not selected:
            raise ResourceUnavailableError("当前没有可用于调度的机器人")

        selected_uuid = selected["uuid"]
        start_site = _robot_start_site(selected, site_positions)
        resolved_path = build_route(requested_sites, start_site=start_site)
        _apply_resolved_route(task, resolved_path)
        task.agv_id = selected_uuid
        task.status = TaskStatus.ASSIGNED
        root = await _active_root(db, task)
        if root is None:
            raise ResourceUnavailableError("任务没有提交阶段生成的 RootBp")
        dispatch_key = _new_dispatch_key(task.id)
        select_block = await db.scalar(
            select(WindBlockRecord).where(
                WindBlockRecord.task_record_id == task.id,
                WindBlockRecord.block_name == "CSelectAgvBp",
            )
        )
        if select_block:
            select_block.status = BlockStatus.SUCCESS
            select_block.started_on = select_block.started_on or now()
            select_block.ended_on = now()
            select_block.output_params = to_json_text({"selectedAgvId": selected_uuid})
            selection_variables = from_json_text(select_block.internal_variables, {})
            selection_variables["selectedAgvId"] = selected_uuid
            select_block.internal_variables = to_json_text(selection_variables)

        _hydrate_first_root(root, selected_uuid, start_site, requested_sites[0])

        await _reserve_sites(db, task, selected_uuid, resolved_path["route"])
        await mark_robot_busy(db, selected_uuid, task.id, start_site)
        create_task_log(db, task.id, f"任务已分配给机器人 {selected_uuid}")

    planned_segments = _root_segments(task, root)
    if not planned_segments:
        raise ResourceUnavailableError("当前 RootBp 没有可执行的地图路径")
    task_input = from_json_text(task.input_params, {})
    operation_plans = build_operation_plans(
        task,
        root,
        planned_segments,
        script_name=task_input.get("scriptName"),
    )
    if not operation_plans:
        raise ResourceUnavailableError("当前 RootBp 没有可拆分的动作子步骤")

    variables["selectedAgvId"] = selected_uuid
    variables["rmfDispatchKey"] = dispatch_key
    variables["rmfPublished"] = False
    root_values = from_json_text(
        root.block_input_params_value,
        {},
    )
    if root_values.get("from") is not None:
        variables["currentSite"] = root_values["from"]
    if root_values.get("to") is not None:
        variables["nextSite"] = root_values["to"]
    variables["currentRootStepIndex"] = _root_step_index(root)
    task.variables = to_json_text(variables)

    # 先持久化任务分配结果，再向 RabbitMQ 投递消息。
    await db.commit()
    rabbit_status = await _publish_assignment_message(
        task,
        selected_uuid,
        operation_plans,
        planned_segments,
        dispatch_key,
        root.block_id,
    )
    return {
        "taskId": str(task.id),
        "status": task.status,
        "agvId": selected_uuid,
        "blockCount": len(operation_plans),
        "rabbitStatus": rabbit_status,
    }


def _new_dispatch_key(task_id: int) -> str:
    return f"task-{task_id}-{uuid4().hex}"


async def _active_root(db: AsyncSession, task: WindTaskRecord) -> WindBlockRecord | None:
    root = await db.scalar(
        select(WindBlockRecord)
        .where(
            WindBlockRecord.task_record_id == task.id,
            WindBlockRecord.block_name == "RootBp",
            WindBlockRecord.status.in_([BlockStatus.CREATED, BlockStatus.RUNNING]),
        )
        .order_by(WindBlockRecord.id.desc())
    )
    return root


def _root_step_index(root: WindBlockRecord | None) -> int:
    if root is None:
        return 0
    return int(from_json_text(root.internal_variables, {}).get("rootStepIndex") or 0)


async def _site_positions(db: AsyncSession) -> dict[str, tuple[int | float | None, int | float | None]]:
    sites = (await db.scalars(select(WorkSite).where(WorkSite.del_ == 0))).all()
    return {site.site_id: (site.row_num, site.column_num) for site in sites}


def _robot_start_site(
    robot: dict,
    site_positions: dict[str, tuple[int | float | None, int | float | None]],
) -> str:
    current_site = robot.get("current_site_id") or robot.get("current_location")
    if not current_site or current_site not in site_positions:
        raise ResourceUnavailableError(
            f"机器人 {robot.get('uuid')} 没有可用于规划的当前库位"
        )
    return str(current_site)


def _apply_resolved_route(task: WindTaskRecord, route: dict) -> None:
    resolved_route = route.get("route", [])
    if not resolved_route:
        raise ResourceUnavailableError("无法生成机器人实际起点")
    params = from_json_text(task.input_params, {})
    requested_sites = params.get("sitePath") or []
    params["from"] = resolved_route[0]
    params["to"] = requested_sites[-1]
    params["sitePath"] = requested_sites
    task.input_params = to_json_text(params)
    task.path = to_json_text(route)


def _root_segments(task: WindTaskRecord, root: WindBlockRecord) -> list[dict]:
    """从完整地图路径中截取当前 RootBp 对应的一段连续路线。"""
    root_values = from_json_text(root.block_input_params_value, {})
    from_site = root_values.get("from")
    to_site = root_values.get("to")
    if not from_site or not to_site:
        return []

    path = from_json_text(task.path, {})
    all_segments = path.get("segments") or []
    selected: list[dict] = []
    started = False
    for segment in all_segments:
        segment_from = segment.get("from")
        segment_to = segment.get("to")
        if not started:
            if segment_from != from_site:
                continue
            started = True
        elif selected[-1].get("to") != segment_from:
            break
        selected.append({"from": segment_from, "to": segment_to})
        if segment_to == to_site:
            break
    if selected and selected[-1].get("to") == to_site:
        return selected
    return [{"from": from_site, "to": to_site}]


def _hydrate_first_root(
    root: WindBlockRecord,
    agv_id: str,
    from_site: str,
    to_site: str,
) -> None:
    if not root:
        raise ResourceUnavailableError("任务缺少第一个 RootBp")
    root_values = from_json_text(root.block_input_params_value, {})
    root_values.update({"from": from_site, "to": to_site})
    root.block_input_params_value = to_json_text(root_values)
    root_variables = from_json_text(root.internal_variables, {})
    root_variables["selectedAgvId"] = agv_id
    root_variables.pop("pendingStart", None)
    root.internal_variables = to_json_text(root_variables)



async def _publish_assignment_message(
    task: WindTaskRecord,
    agv_id: str,
    operation_plans: list[dict],
    planned_segments: list[dict],
    dispatch_key: str,
    root_block_id: str,
) -> str:
    payload = {
        "event": "task.assigned",
        "taskId": str(task.id),
        "agvId": agv_id,
        "blockCount": len(operation_plans),
        "rootBlockId": root_block_id,
        "dispatchKey": dispatch_key,
        "segments": planned_segments,
        "operations": operation_plans,
    }
    try:
        await publish_rmf_dispatch(payload)
    except Exception:
        logger.exception("任务分配结果已保存，但 RabbitMQ 投递失败：task_id=%s", task.id)
        return "PENDING_RABBITMQ"
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


async def _reserve_sites(db: AsyncSession, task: WindTaskRecord, agv_id: str, route: list[str]) -> None:
    site_ids = {site_id for site_id in route if site_id}
    if not site_ids:
        return
    sites = (
        await db.scalars(select(WorkSite).where(WorkSite.site_id.in_(site_ids)))
    ).all()
    for site in sites:
        site.preparing = 1
        site.agv_id = agv_id
        site.holder = 3
