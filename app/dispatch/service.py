from __future__ import annotations

from datetime import timedelta
import logging
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import build_operation_plans, create_task_log, mark_robot_busy, task_requested_sites
from app.map.graph import MapGraph
from app.map.service import get_map_graph
from common.enums.block_status import BlockStatus
from common.enums.task_status import TaskStatus
from common.exception.base import (
    MapNodeNotFoundError,
    MapRouteNotFoundError,
    ResourceUnavailableError,
    StatusNotAllowedError,
    TaskNotFoundError,
)
from common.utils import build_route, from_json_text, now, to_json_text
from core.conf import settings
from database.models import MapNode, RobotCurrentState, RobotItem, WindBlockRecord, WindTaskRecord
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
        raise ResourceUnavailableError("任务没有可执行的目标地图节点")

    map_version, map_graph = await get_map_graph(db, task.map_version_id)
    map_graph.require_nodes(requested_sites)
    task.map_version_id = map_version.id

    if reuse_assignment:
        selected_uuid = str(task.agv_id)
        root = await _active_root(db, task)
        if root is None:
            raise ResourceUnavailableError("任务没有待下发的 RootBp")
        dispatch_key = variables.get("rmfDispatchKey") or _new_dispatch_key(task.id)
    else:
        candidates = await _robot_candidates(db)
        route_costs = _candidate_route_costs(candidates, map_graph, requested_sites[0])
        selected = solver.choose_robot(
            candidates,
            agv_id or task.agv_id,
            target_site=requested_sites[0],
            route_costs=route_costs,
        )
        if not selected:
            raise ResourceUnavailableError("当前没有可用于调度的机器人")

        selected_uuid = selected["uuid"]
        start_site = _robot_start_site(selected, map_graph)
        resolved_path = build_route(
            requested_sites,
            start_site=start_site,
            map_data=map_graph,
        )
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

        target_index = _prepare_first_root(root, selected_uuid, start_site, requested_sites)
        if target_index is None:
            root.status = BlockStatus.SUCCESS
            root.started_on = root.started_on or now()
            root.ended_on = now()
            root.output_params = to_json_text({"arrived": True, "currentSite": start_site})
            task.status = TaskStatus.COMPLETED
            task.ended_on = now()
            variables["selectedAgvId"] = selected_uuid
            variables["currentSite"] = start_site
            variables["nextSite"] = None
            variables["currentStepIndex"] = len(requested_sites)
            variables["totalSteps"] = len(requested_sites)
            task.variables = to_json_text(variables)
            create_task_log(db, task.id, "机器人已位于全部目标点，任务直接完成")
            await db.commit()
            return {
                "taskId": str(task.id),
                "status": task.status,
                "agvId": selected_uuid,
                "mapVersionId": str(map_version.id),
                "blockCount": 0,
                "rabbitStatus": "SKIPPED_ALREADY_AT_TARGET",
            }

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
        "mapVersionId": str(task.map_version_id),
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


def _candidate_route_costs(
    candidates: list[dict],
    map_graph: MapGraph,
    target_site: str,
) -> dict[str, float | None]:
    costs: dict[str, float | None] = {}
    for robot in candidates:
        uuid = str(robot.get("uuid") or "")
        current_site = str(robot.get("current_site_id") or "").strip()
        if not uuid or not current_site or not map_graph.has_node(current_site):
            costs[uuid] = None
            continue
        try:
            costs[uuid] = map_graph.path_cost(current_site, target_site)
        except (MapNodeNotFoundError, MapRouteNotFoundError):
            costs[uuid] = None
    return costs


def _robot_start_site(
    robot: dict,
    map_graph: MapGraph,
) -> str:
    current_site = str(robot.get("current_site_id") or "").strip()
    if not current_site:
        raise ResourceUnavailableError(
            f"机器人 {robot.get('uuid')} 没有可用于规划的当前地图节点"
        )
    if not map_graph.has_node(current_site):
        raise MapNodeNotFoundError(
            f"机器人当前地图节点不存在: {current_site}"
        )
    return current_site


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
    raise MapRouteNotFoundError(
        f"任务保存的地图路径不完整: {from_site} -> {to_site}"
    )


def _prepare_first_root(
    root: WindBlockRecord,
    agv_id: str,
    start_site: str,
    requested_sites: list[str],
) -> int | None:
    """补齐第一个 RootBp，并跳过机器人已经到达的连续目标点。"""
    if not root:
        raise ResourceUnavailableError("任务缺少第一个 RootBp")
    root_variables = from_json_text(root.internal_variables, {})
    target_index = int(root_variables.get("targetIndex") or 0)
    while target_index < len(requested_sites) and requested_sites[target_index] == start_site:
        target_index += 1
    if target_index >= len(requested_sites):
        _hydrate_first_root(root, agv_id, start_site, start_site, target_index)
        return None
    _hydrate_first_root(root, agv_id, start_site, requested_sites[target_index], target_index)
    return target_index


def _hydrate_first_root(
    root: WindBlockRecord,
    agv_id: str,
    from_site: str,
    to_site: str,
    target_index: int,
) -> None:
    if not root:
        raise ResourceUnavailableError("任务缺少第一个 RootBp")
    root_values = from_json_text(root.block_input_params_value, {})
    root_values.update({"from": from_site, "to": to_site})
    root.block_input_params_value = to_json_text(root_values)
    root_variables = from_json_text(root.internal_variables, {})
    root_variables["selectedAgvId"] = agv_id
    root_variables["targetIndex"] = target_index
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
        "mapVersionId": str(task.map_version_id) if task.map_version_id else None,
        "blockCount": len(operation_plans),
        "rootBlockId": root_block_id,
        "dispatchKey": dispatch_key,
        "root": {
            "from": planned_segments[0]["from"] if planned_segments else None,
            "to": planned_segments[-1]["to"] if planned_segments else None,
        },
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
    """
    查询一个待调度的任务，并加行锁防止并发冲突。
    如果指定 task_id，则尝试锁定该任务；否则按优先级和时间顺序自动选取。
    :param db:
    :param task_id:
    :return:
    """
    if task_id is not None:
        task = await db.scalar(
            select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update()  #with_for_update()查询的时候加个行级锁，锁定该行防止别的事务更改
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
    node_codes = {node_code for node_code in route if node_code}
    if not node_codes:
        return
    nodes = (
        await db.scalars(
            select(MapNode).where(
                MapNode.map_version_id == task.map_version_id,
                MapNode.node_code.in_(node_codes),
                MapNode.del_ == 0,
            )
        )
    ).all()
    for node in nodes:
        node.preparing = 1
        node.agv_id = agv_id
        node.holder = 3
