from __future__ import annotations

from datetime import timedelta
import logging
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import (
    create_next_operation_block,
    create_root_block,
    create_select_robot_block,
    create_task_log,
    mark_robot_busy,
    task_requested_sites,
)
from app.map.graph import MapGraph, weighted_entry_cost
from app.map.service import get_active_map_version, get_map_graph
from common.enums.block_status import BlockStatus
from common.enums.dispatch_status import DispatchStatus
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
    task = await _pick_task(db=db, task_id=task_id)
    if task.status not in {TaskStatus.PENDING_ASSIGN, TaskStatus.ASSIGNED, TaskStatus.SUSPENDED} and not force:
        raise StatusNotAllowedError("当前任务状态不允许调度")

    variables = from_json_text(task.variables, {})
    reuse_assignment = task.status == TaskStatus.ASSIGNED and bool(task.agv_id) and not force
    requested_sites = task_requested_sites(task)
    if not requested_sites:
        raise ResourceUnavailableError("任务中请求路径为空")

    await _ensure_task_map_version_is_active(db=db, task=task)
    map_version, map_graph = await get_map_graph(db=db, map_version_id=task.map_version_id)
    map_graph.require_nodes(requested_sites)

    if reuse_assignment: #任务机器人已经指定
        selected_agv_id = str(task.agv_id)
        root = await _active_root(db, task)
        if root is None:
            raise ResourceUnavailableError("任务没有待下发的 RootBp")
        dispatch_key = variables.get("rmfDispatchKey") or _new_dispatch_key(task.id)
    else:
        # 提交任务时不创建流程块；第一次调度时先创建 RootBp 和选车块占位记录。
        root = await create_root_block(
            session=db,
            task=task,
            root_step_index=1,
            target_index=0,
            from_site=None,
            to_site=requested_sites[0],
        )
        await create_select_robot_block(session=db, task=task, root=root)
        robots = await _robot_candidates(db=db)
        target_site = requested_sites[0]
        required_uuid = agv_id or task.agv_id
        start_resolutions: dict[str, dict] = {} #各个候选机器人的“起点解析结果”
        route_costs = _candidate_route_costs(
            robots=robots,
            map_graph=map_graph,
            target_site=target_site,
            start_resolutions=start_resolutions,
        )
        selected = solver.choose_robot(
            robots=robots,
            required_uuid=required_uuid,
            target_site=target_site,
            route_costs=route_costs,
        )
        if not selected:
            raise ResourceUnavailableError("当前没有可用于调度的机器人")

        selected_agv_id = selected["uuid"]
        start_resolution = start_resolutions.get(selected_agv_id)

        if start_resolution is None:
            raise ResourceUnavailableError(
                f"选中的机器人缺少有效起点解析结果：agv_id={selected_agv_id}"
            )

        start_site = start_resolution["entryNode"]  #离机器人最近的起点
        resolved_path = build_route(
            site_path=requested_sites,
            start_site=start_site,
            map_data=map_graph,
            start_pose=start_resolution.get("startPose"),
            entry_node=start_site,
            is_at_site=start_resolution["isAtSite"],
        )   # 这是加上了当前机器人最近的小车库位点的库位或小车位置坐标

        _apply_resolved_route(task, resolved_path)

        task.agv_id = selected_agv_id
        task.status = TaskStatus.ASSIGNED
        dispatch_key = _new_dispatch_key(task.id)

        target_index = _prepare_first_root(
            root=root,
            agv_id=selected_agv_id,
            start_site=start_site,
            requested_sites=requested_sites,
            is_at_site=start_resolution["isAtSite"],
        )   #target_index当前任务正在处理 WMS 目标列表中的第几个目标点
        if target_index is None:
            root.status = BlockStatus.SUCCESS
            root.started_on = root.started_on or now()
            root.ended_on = now()
            root.output_params = to_json_text({"arrived": True, "currentSite": start_site})
            task.status = TaskStatus.COMPLETED
            task.ended_on = now()
            variables["selectedAgvId"] = selected_agv_id
            variables["currentSite"] = start_site
            variables["nextSite"] = None
            variables["currentStepIndex"] = len(requested_sites)
            variables["totalSteps"] = len(requested_sites)
            task.variables = to_json_text(variables)
            await create_select_robot_block(
                session=db,
                task=task,
                root=root,
                selected_agv_id=selected_agv_id,
            )
            create_task_log(db, task.id, "机器人已位于全部目标点，任务直接完成")
            await db.commit()
            return {
                "taskId": str(task.id),
                "status": task.status,
                "agvId": selected_agv_id,
                "mapVersionId": str(map_version.id),
                "blockCount": 0,
                "rabbitStatus": "SKIPPED_ALREADY_AT_TARGET",
            }

        await create_select_robot_block(
            session=db,
            task=task,
            root=root,
            selected_agv_id=selected_agv_id,
        )
        await _reserve_sites(
            db=db,
            task=task,
            agv_id=selected_agv_id,
            route=resolved_path["route"],
        )
        await mark_robot_busy(
            session=db,
            uuid=selected_agv_id,
            task_id=task.id,
            current_site_id=start_site if start_resolution["isAtSite"] else None,
        )
        create_task_log(db, task.id, f"任务已分配给机器人 {selected_agv_id}")

    planned_segments = _root_segments(task=task, root=root)
    if not planned_segments:
        raise ResourceUnavailableError("当前 RootBp 没有可执行的地图路径")
    operation_block = await create_next_operation_block(
        session=db,
        task=task,
        root=root,
        segments=planned_segments,
    )
    if operation_block is None:
        raise ResourceUnavailableError("当前 RootBp 没有可创建的移动步骤")
    operation_input = from_json_text(operation_block.block_input_params_value, {})
    operation_variables = from_json_text(operation_block.internal_variables, {})
    planned_segment = {
        "from": operation_input.get("from"),
        "to": operation_input.get("to"),
        "segmentType": operation_input.get("segmentType", "map"),
        "startPose": operation_input.get("startPose"),
        "stepIndex": int(operation_variables.get("stepIndex") or 1),
    }

    variables["selectedAgvId"] = selected_agv_id
    variables["rmfDispatchKey"] = dispatch_key
    variables["rmfPublished"] = False
    variables["currentRootStepIndex"] = _root_step_index(root)
    variables["currentOperationIndex"] = planned_segment["stepIndex"]
    if operation_input.get("from") is not None:
        variables["currentSite"] = operation_input["from"]
    variables["nextSite"] = operation_input.get("to")
    task.variables = to_json_text(variables)

    # 先持久化当前 RootBp 和这一条移动步骤，再向 RabbitMQ 投递消息。
    await _ensure_task_map_version_is_active(db=db, task=task)
    await db.commit()
    rabbit_status = await _publish_assignment_message(
        task=task,
        agv_id=selected_agv_id,
        operation_block=operation_block,
        planned_segment=planned_segment,
        dispatch_key=dispatch_key,
        root_block_id=root.block_id,
    )
    return {
        "taskId": str(task.id),
        "status": task.status,
        "agvId": selected_agv_id,
        "mapVersionId": str(task.map_version_id),
        "blockCount": 1,
        "operationBlockId": operation_block.block_id,
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
    robots: list[dict],
    map_graph: MapGraph,
    target_site: str,
    start_resolutions: dict[str, dict] | None = None,
) -> dict[str, float | None]:
    """
    计算每个候选机器人到目标节点的综合路径代价。

    机器人已经匹配到地图节点时，成本是该节点到目标节点的地图路径代价；
    机器人只有坐标时，成本是入口节点到目标节点的地图代价乘 0.6，再加坐标
    到入口节点欧氏距离乘 0.4。无法定位或不存在可行路径的机器人会记录为 None。

    :param robots: 候选机器人状态字典列表。
    :param map_graph: 任务绑定地图版本对应的内存地图图对象。
    :param target_site: 任务第一个目标地图节点编码。
    :param start_resolutions: 可选的机器人起点解析结果缓存，供选车后复用同一个入口节点。
    :return: 机器人编码到路径代价的映射，无法规划时对应值为 None。
    """
    costs: dict[str, float | None] = {}
    for robot in robots:
        uuid = str(robot.get("uuid") or "")
        if not uuid:
            continue
        resolution = _robot_start_site(
            robot=robot,
            map_graph=map_graph,
            target_site=target_site,
        )
        if resolution is None:
            costs[uuid] = None
            continue
        costs[uuid] = float(resolution["totalCost"])
        if start_resolutions is not None:
            start_resolutions[uuid] = resolution
    return costs


def _robot_start_site(
    robot: dict,
    map_graph: MapGraph,
    target_site: str,
) -> dict | None:
    """
    解析机器人执行当前任务时使用的地图接入起点。

    如果机器人有有效的 ``current_site_id``，直接使用该节点；如果只有
    ``current_x/current_y``，则在所有能够到达目标节点的路径入口中选择综合代价最低者。

    :param robot: 包含机器人当前状态的字典。
    :param map_graph: 任务绑定地图版本对应的内存地图图对象。
    :param target_site: 任务第一个目标地图节点编码。
    :return: 起点解析结果；其中 ``isAtSite`` 明确表示机器人是否已经位于当前节点；
        没有坐标、节点或可行路径时返回 None。
    """
    current_site = str(robot.get("current_site_id") or "").strip()
    current_x = robot.get("current_x")
    current_y = robot.get("current_y")

    if current_site:
        if not map_graph.has_node(current_site):
            return None
        try:
            route_cost = map_graph.path_cost(current_site, target_site)
        except (MapNodeNotFoundError, MapRouteNotFoundError):
            return None
        return {
            "entryNode": current_site,  #机器人当前所在的地图节点
            "routeCost": float(route_cost),
            "approachDistance": 0.0, #机器人真实坐标到地图接入节点的距离。因为机器人已经在节点上，所以是 0.0
            "totalCost": weighted_entry_cost(route_cost, 0.0),
            "startPose": {"x": float(current_x), "y": float(current_y)}, #机器人真实坐标
            "isAtSite": True,
        }

    if current_x is None or current_y is None:
        return None
    try:
        current_x = float(current_x)
        current_y = float(current_y)
    except (TypeError, ValueError):
        return None

    candidate = map_graph.nearest_entry_node(target_site, current_x, current_y)
    if candidate is None:
        return None
    return {
        "entryNode": candidate.node_code,
        "routeCost": candidate.route_cost,
        "approachDistance": candidate.coordinate_distance,
        "totalCost": candidate.total_cost,
        "startPose": {"x": current_x, "y": current_y},
        "isAtSite": False,
    }


def _apply_resolved_route(task: WindTaskRecord, route: dict) -> None:
    """
    将包含机器人实际起点的完整路径写回任务输入参数和路径快照(task的input_params和path)。

    :param task: 要更新的任务 ORM 对象。
    :param route: 路径规划结果，至少包含 route、sitePath 和 segments 字段。
    :return: 无返回值。
    """
    resolved_route = route.get("route", [])
    if not resolved_route:
        raise ResourceUnavailableError("无法生成机器人实际起点")
    params = from_json_text(task.input_params, {})
    requested_sites = params.get("sitePath") or []
    params["from"] = (
        route.get("entryNode")
        if route.get("isAtSite", True)
        else None
    )
    params["to"] = requested_sites[-1]
    params["sitePath"] = requested_sites
    if route.get("entryNode"):
        params["entryNode"] = route["entryNode"]
    if route.get("startPose") is not None:
        params["startPose"] = route["startPose"]
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
    if to_site is None:
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
        selected.append(dict(segment))
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
    is_at_site: bool = True,
) -> int | None:
    """
    补齐第一个 RootBp，并跳过机器人已经到达的连续目标点。

    :param root: 提交任务阶段创建的第一个 RootBp。
    :param agv_id: 已选中的机器人编码。
    :param start_site: 机器人当前所在的地图节点编码。
    :param requested_sites: 任务按顺序提交的目标节点列表。
    :param is_at_site: 机器人是否已经确认位于 start_site；只有为 True 时才能跳过相同目标。
    :return: 第一个尚未到达的目标节点下标；全部到达时返回 None。
    """
    if not root:
        raise ResourceUnavailableError("任务缺少第一个 RootBp")
    root_variables = from_json_text(root.internal_variables, {})
    target_index = int(root_variables.get("targetIndex") or 0)
    if is_at_site:
        while target_index < len(requested_sites) and requested_sites[target_index] == start_site:
            target_index += 1
    if target_index >= len(requested_sites):
        _hydrate_first_root(root, agv_id, start_site, start_site, target_index, is_at_site)
        return None
    root_from = start_site if is_at_site else None
    _hydrate_first_root(
        root,
        agv_id,
        root_from,
        requested_sites[target_index],
        target_index,
        is_at_site,
    )
    return target_index


def _hydrate_first_root(
    root: WindBlockRecord,
    agv_id: str,
    from_site: str | None,
    to_site: str,
    target_index: int,
    is_at_site: bool = True,
) -> None:
    """
    把机器人、起点、终点和目标下标写入第一个 RootBp。

    :param root: 要补充信息的 RootBp 流程块。
    :param agv_id: 已选中的机器人编码。
    :param from_site: 当前 RootBp 的起点节点编码。
    :param to_site: 当前 RootBp 的终点节点编码。
    :param target_index: 终点在任务目标节点列表中的下标。
    :param is_at_site: 机器人是否已经确认位于 from_site。
    :return: 无返回值。
    """
    if not root:
        raise ResourceUnavailableError("任务缺少第一个 RootBp")
    root_values = from_json_text(root.block_input_params_value, {})
    root_values.update(
        {
            "from": from_site,
            "to": to_site,
            "pendingStart": not is_at_site,
        }
    )
    root.block_input_params_value = to_json_text(root_values)
    root_variables = from_json_text(root.internal_variables, {})
    root_variables["selectedAgvId"] = agv_id
    root_variables["targetIndex"] = target_index
    root.internal_variables = to_json_text(root_variables)



async def _publish_assignment_message(
    task: WindTaskRecord,
    agv_id: str,
    operation_block: WindBlockRecord,
    planned_segment: dict,
    dispatch_key: str,
    root_block_id: str,
) -> str:
    """
    组装并发布任务分配消息到 RabbitMQ，供 RMF 消费者继续下发。

    :param task: 已完成机器人分配和路径规划的任务 ORM 对象。
    :param agv_id: 已选中的机器人编码。
    :param operation_block: 当前要发送的 CAgvOperationBp 数据库记录。
    :param planned_segment: 当前要发送的一条地图路径分段。
    :param dispatch_key: 本次调度投递的唯一调度键。
    :param root_block_id: 当前 RootBp 的流程块编码。
    :return: RabbitMQ 投递状态，成功时为 PUBLISHED，失败时为 PENDING_RABBITMQ。
    """
    operation = {
        "blockId": operation_block.block_id,
        "parentBlockId": operation_block.parent_block_id,
        "blockName": operation_block.block_name,
        "orderId": operation_block.order_id,
        "inputParams": from_json_text(operation_block.block_input_params_value, {}),
        "internalVariables": from_json_text(operation_block.internal_variables, {}),
    }
    payload = {
        "event": "task.assigned",
        "taskId": str(task.id),
        "agvId": agv_id,
        "mapVersionId": str(task.map_version_id) if task.map_version_id else None,
        "blockCount": 1,
        "rootBlockId": root_block_id,
        "dispatchKey": dispatch_key,
        "entryNode": from_json_text(task.path, {}).get("entryNode"),
        "startPose": from_json_text(task.path, {}).get("startPose"),
        "root": {
            "from": planned_segment.get("from"),
            "to": planned_segment.get("to"),
        },
        "segments": [planned_segment],
        "operations": [operation],
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
        if (
            not state
            or state.last_heartbeat_at is None
            or state.last_heartbeat_at < heartbeat_cutoff
            or state.current_status != RobotStatus.IDLE
            or state.dispatch_status != DispatchStatus.IDLE
            or state.has_unresolved_alarm != 0
            or robot.enable_status != 1
            or float(state.battery_level or 0) < float(robot.battery_threshold or 0)
            or not (
                state.current_site_id
                or (state.current_x is not None and state.current_y is not None)
            )
        ):
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
