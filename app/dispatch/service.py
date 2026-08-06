from __future__ import annotations

from datetime import timedelta
import logging
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import build_operation_plans, create_task_log, mark_robot_busy, task_requested_sites
from app.map.graph import MapGraph
from app.map.service import get_active_map_version, get_map_graph
from common.enums.block_status import BlockStatus
from common.enums.robot_status import RobotStatus
from common.enums.task_status import TaskStatus
from common.exception.base import (
    MapNodeNotFoundError,
    MapRouteNotFoundError,
    MapVersionUnavailableError,
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
    """
    为任务选择机器人、基于绑定地图版本规划路径、预占资源并投递 RMF 调度消息。

    方法会先校验任务状态和地图版本，再根据任务是否已经分配机器人决定复用原分配结果，
    或重新执行选车、路径规划、流程块准备和 RabbitMQ 投递。

    :param db: 当前使用的异步数据库会话。
    :param task_id: 指定调度的任务主键；为空时自动选择待调度任务。
    :param agv_id: 指定使用的机器人编码；为空时由调度器自动选择机器人。
    :param force: 是否强制跳过部分任务状态校验。
    :return: 任务分配、地图版本、流程块数量和消息投递状态。
    """
    task = await _pick_task(db, task_id)
    if task.status not in {TaskStatus.PENDING_ASSIGN, TaskStatus.ASSIGNED, TaskStatus.SUSPENDED} and not force:
        raise StatusNotAllowedError("当前任务状态不允许调度")

    variables = from_json_text(task.variables, {})
    reuse_assignment = task.status == TaskStatus.ASSIGNED and bool(task.agv_id) and not force
    requested_sites = task_requested_sites(task)
    if not requested_sites:
        raise ResourceUnavailableError("任务中请求路径为空")

    await _ensure_task_map_version_is_active(db, task)
    map_version, map_graph = await get_map_graph(db, task.map_version_id)
    map_graph.require_nodes(requested_sites)

    if reuse_assignment: #任务机器人已经指定
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
    await _ensure_task_map_version_is_active(db, task)
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
    """
    为任务生成一次 RMF 调度投递使用的唯一调度键。

    :param task_id: 任务主键。
    :return: 由任务 ID 和随机 UUID 组成的调度键。
    """
    return f"task-{task_id}-{uuid4().hex}"


async def _ensure_task_map_version_is_active(db: AsyncSession, task: WindTaskRecord) -> None:
    """
    校验任务绑定的地图版本仍然是当前激活版本，避免使用旧地图继续调度。

    :param db: 当前使用的异步数据库会话。
    :param task: 待校验的任务 ORM 对象。
    :return: 无返回值；地图版本不可用或已切换时抛出业务异常。
    """
    if task.map_version_id is None:
        raise MapVersionUnavailableError("任务未绑定地图版本，无法继续调度")

    active_version = await get_active_map_version(db)
    if active_version.id != task.map_version_id:
        raise MapVersionUnavailableError(
            f"任务绑定的地图版本已失效，taskMapVersionId={task.map_version_id}, "
            f"activeMapVersionId={active_version.id}",
            data={
                "taskMapVersionId": task.map_version_id,
                "activeMapVersionId": active_version.id,
            },
        )


async def _active_root(db: AsyncSession, task: WindTaskRecord) -> WindBlockRecord | None:
    """
    查询任务当前处于创建或运行状态的 RootBp 流程块。

    :param db: 当前使用的异步数据库会话。
    :param task: 要查询流程块的任务 ORM 对象。
    :return: 当前有效的 RootBp，没有找到时返回 None。
    """
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
    """
    从 RootBp 的内部变量中读取当前 RootBp 的步骤序号。

    :param root: RootBp 流程块对象，可以为空。
    :return: 当前 RootBp 步骤序号，没有流程块或没有记录时返回 0。
    """
    if root is None:
        return 0
    return int(from_json_text(root.internal_variables, {}).get("rootStepIndex") or 0)


def _candidate_route_costs(
    candidates: list[dict],
    map_graph: MapGraph,
    target_site: str,
) -> dict[str, float | None]:
    """
    计算每个候选机器人当前位置到目标节点的最小地图路径代价。

    无当前位置、当前位置不在地图中或不存在可行路径的机器人会记录为 None，
    调度器会据此过滤不可用机器人。

    :param candidates: 候选机器人状态字典列表。
    :param map_graph: 任务绑定地图版本对应的内存地图图对象。
    :param target_site: 任务第一个目标地图节点编码。
    :return: 机器人编码到路径代价的映射，无法规划时对应值为 None。
    """
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
    """
    获取并校验机器人当前所在的地图节点。

    :param robot: 包含机器人当前状态的字典。
    :param map_graph: 任务绑定地图版本对应的内存地图图对象。
    :return: 机器人当前所在的地图节点编码。
    """
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
    """
    将包含机器人实际起点的完整路径写回任务输入参数和路径快照。

    :param task: 要更新的任务 ORM 对象。
    :param route: 路径规划结果，至少包含 route、sitePath 和 segments 字段。
    :return: 无返回值。
    """
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
    """
    从任务保存的完整地图路径中截取当前 RootBp 对应的一段连续路线。

    :param task: 保存完整路径信息的任务 ORM 对象。
    :param root: 当前需要下发的 RootBp 流程块。
    :return: 当前 RootBp 对应的连续路径边列表。
    """
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
    """
    补齐第一个 RootBp，并跳过机器人已经到达的连续目标点。

    :param root: 提交任务阶段创建的第一个 RootBp。
    :param agv_id: 已选中的机器人编码。
    :param start_site: 机器人当前所在的地图节点编码。
    :param requested_sites: 任务按顺序提交的目标节点列表。
    :return: 第一个尚未到达的目标节点下标；全部到达时返回 None。
    """
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
    """
    把机器人、起点、终点和目标下标写入第一个 RootBp。

    :param root: 要补充信息的 RootBp 流程块。
    :param agv_id: 已选中的机器人编码。
    :param from_site: 当前 RootBp 的起点节点编码。
    :param to_site: 当前 RootBp 的终点节点编码。
    :param target_index: 终点在任务目标节点列表中的下标。
    :return: 无返回值。
    """
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
    """
    组装并发布任务分配消息到 RabbitMQ，供 RMF 消费者继续下发。

    :param task: 已完成机器人分配和路径规划的任务 ORM 对象。
    :param agv_id: 已选中的机器人编码。
    :param operation_plans: 当前 RootBp 拆分出的动作子步骤。
    :param planned_segments: 当前 RootBp 对应的地图路径边列表。
    :param dispatch_key: 本次调度投递的唯一调度键。
    :param root_block_id: 当前 RootBp 的流程块编码。
    :return: RabbitMQ 投递状态，成功时为 PUBLISHED，失败时为 PENDING_RABBITMQ。
    """
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
    查询一个待调度的任务，并加行锁防止并发调度冲突。

    指定 task_id 时锁定指定任务；未指定时按优先级倒序和创建时间正序自动选取任务。

    :param db: 当前使用的异步数据库会话。
    :param task_id: 指定任务主键；为空时自动选择待分配任务。
    :return: 被选中的任务 ORM 对象。
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
    """
    查询满足基础在线条件的机器人，并合并机器人档案和实时状态。

    只有存在实时状态且最近心跳未超时的机器人会进入候选列表。

    :param db: 当前使用的异步数据库会话。
    :return: 可参与本次调度的机器人状态字典列表。
    """
    robots = (await db.scalars(select(RobotItem).where(RobotItem.del_ == 0))).all()
    states: dict[str, RobotCurrentState] = {
        row.uuid: row
        for row in (await db.scalars(select(RobotCurrentState))).all()
    }

    heartbeat_cutoff = now() - timedelta(seconds=settings.robot_heartbeat_timeout_seconds)
    payload = []
    for robot in robots:
        state = states.get(robot.uuid)
        if not state or state.last_heartbeat_at is None or state.last_heartbeat_at < heartbeat_cutoff or state.current_status != RobotStatus.IDLE:
            continue
        merged = robot.to_dict()
        merged.update(state.to_dict())
        payload.append(merged)
    return payload


async def _reserve_sites(db: AsyncSession, task: WindTaskRecord, agv_id: str, route: list[str]) -> None:
    """
    预占任务完整路径上的地图节点，标记节点正在被任务预留。

    :param db: 当前使用的异步数据库会话。
    :param task: 当前调度任务 ORM 对象，用于确定地图版本。
    :param agv_id: 执行任务的机器人编码。
    :param route: 机器人从起点到任务目标的完整节点路径。
    :return: 无返回值；修改结果会随当前数据库事务提交。
    """
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
