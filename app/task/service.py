from __future__ import annotations

import logging

from redis.exceptions import RedisError as RedisClientError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import create_task_log, mark_robot_idle, serialize_task
from common.enums.task_status import TaskStatus
from common.exception.base import ResourceUnavailableError, RobotNotFoundError, SiteNotFoundError, StatusNotAllowedError, TaskNotFoundError, TemplateNotFoundError
from common.utils import build_route, from_json_text, normalize_site_path, now, paginate, to_json_text
from core.conf import settings
from database.models import RobotItem, WindBlockRecord, WindTaskDef, WindTaskLog, WindTaskRecord, WorkSite
from database.redis import get_redis
from scheduler.queue import TaskQueue


logger = logging.getLogger(__name__)


async def submit_task(
    db: AsyncSession,
    template_label: str | None,
    site_path: list[str],
    priority: int,
    agv_id: str | None,
    out_order_no: str | None,
    periodic_task: int,
    remark: str | None,
) -> dict:
    """
    校验任务所需资源，创建任务快照、路径信息和初始任务日志。
    """
    # 起点依赖调度阶段选中的机器人当前位置，所以提交时只校验 WMS 目标路径。
    requested_sites = normalize_site_path(site_path)
    if not requested_sites:
        raise SiteNotFoundError("WMS 路径不能为空")
    for site_id in requested_sites:
        await _get_available_site(db, site_id)
    task_def = await _get_task_def(db, template_label or settings.default_task_label)
    if agv_id:
        await _get_robot(db, agv_id)

    # 保存请求路径，但不提前生成最终 segments；机器人起点要等调度选车后才能确定。
    route = build_route(requested_sites)
    variables = {
        "currentStepIndex": 0,
        "totalSteps": 0,
        "currentSite": None,
        "nextSite": requested_sites[0],
        "selectedAgvId": agv_id or "",
        "requestedSites": requested_sites,
        "remark": remark or "",
    }
    input_params = {
        "from": None,
        "to": requested_sites[-1],
        "sitePath": requested_sites,
        "vehicle": agv_id or "",
        "priority": priority,
        "remark": remark or "",
    }

    task = WindTaskRecord(
        def_id=task_def.id,
        def_label=task_def.label,
        def_version=task_def.version,
        status=TaskStatus.PENDING_ASSIGN,
        input_params=to_json_text(input_params),
        path=to_json_text(route),
        variables=to_json_text(variables),
        task_def_detail=task_def.detail,
        agv_id=agv_id,
        out_order_no=out_order_no,
        periodic_task=periodic_task,
        priority=priority,
    )
    db.add(task)
    await db.flush()
    create_task_log(db, task.id, f"任务创建成功，WMS 路径={requested_sites}")
    await db.commit()
    await db.refresh(task)
    queue_status = await _enqueue_after_commit(task)
    return {
        "taskId": str(task.id),
        "status": task.status,
        "fromSite": None,
        "toSite": requested_sites[-1],
        "sitePath": requested_sites,
        "agvId": agv_id,
        "outOrderNo": out_order_no,
        "priority": priority,
        "createdOn": task.to_dict()["created_on"],
        "queueStatus": queue_status,
    }


async def get_task_detail(db: AsyncSession, task_id: int) -> dict:
    """
    查询任务详情，并加载任务对应的流程块和日志记录。
    """
    task = await _get_task(db, task_id)
    blocks = (
        await db.scalars(
            select(WindBlockRecord).where(WindBlockRecord.task_record_id == task.id).order_by(WindBlockRecord.id.asc())
        )
    ).all()
    logs = (
        await db.scalars(
            select(WindTaskLog).where(WindTaskLog.task_record_id == task.id).order_by(WindTaskLog.create_time.asc())
        )
    ).all()
    return serialize_task(task, blocks, logs)


async def list_tasks(
    db: AsyncSession,
    page: int,
    page_size: int,
    status: int | None = None,
    agv_id: str | None = None,
    from_site: str | None = None,
    to_site: str | None = None,
    keyword: str | None = None,
) -> dict:
    """
    查询任务记录，按筛选条件过滤后返回分页任务列表。
    """
    tasks = (await db.scalars(select(WindTaskRecord).order_by(WindTaskRecord.created_on.desc()))).all()
    items = []
    for task in tasks:
        payload = serialize_task(task)
        if status is not None and payload["status"] != status:
            continue
        if agv_id and payload["agvId"] != agv_id:
            continue
        if from_site and payload["fromSite"] != from_site:
            continue
        if to_site and payload["toSite"] != to_site:
            continue
        if keyword:
            keyword_hit = keyword in (payload["taskId"] or "") or keyword in (payload["outOrderNo"] or "")
            if not keyword_hit:
                continue
        items.append(payload)
    return paginate(items, page, page_size)


async def cancel_task(db: AsyncSession, task_id: int, reason: str | None) -> dict:
    """
    校验任务是否允许取消，更新任务结束状态并释放机器人资源。
    """
    task = await _get_task(db, task_id)
    if task.status in {TaskStatus.COMPLETED, TaskStatus.CANCELLED}:
        raise StatusNotAllowedError("当前任务状态不允许取消")
    task.status = TaskStatus.CANCELLED
    task.ended_on = now()
    task.ended_reason = reason or "manual cancel"
    create_task_log(db, task.id, f"任务已取消：{task.ended_reason}")
    if task.agv_id:
        await mark_robot_idle(db, task.agv_id, _task_target_site(task))
    await db.commit()
    await _remove_from_queue(task.id)
    return {"taskId": str(task.id), "status": task.status, "endedOn": task.to_dict()["ended_on"]}


async def retry_task(db: AsyncSession, task_id: int, reason: str | None) -> dict:
    """
    将允许重试的源任务复制成新的待调度任务，并建立任务追溯关系。
    """
    # 重试不是原任务复活，而是复制一份新任务，保留源任务追溯关系。
    source = await _get_task(db, task_id)
    if source.status not in {TaskStatus.CANCELLED, TaskStatus.FAILED, TaskStatus.SUSPENDED}:
        raise StatusNotAllowedError("当前任务状态不允许重试")
    cloned = WindTaskRecord(
        def_id=source.def_id,
        def_label=source.def_label,
        def_version=source.def_version,
        status=TaskStatus.PENDING_ASSIGN,
        input_params=source.input_params,
        path=source.path,
        variables=source.variables,
        task_def_detail=source.task_def_detail,
        agv_id=None,
        out_order_no=source.out_order_no,
        periodic_task=source.periodic_task,
        priority=source.priority,
        root_task_record_id=source.root_task_record_id or source.id,
    )
    variables = from_json_text(cloned.variables, {})
    variables["selectedAgvId"] = ""
    variables["retryReason"] = reason or ""
    cloned.variables = to_json_text(variables)
    db.add(cloned)
    await db.flush()
    create_task_log(db, cloned.id, f"任务由 {source.id} 重试创建")
    create_task_log(db, source.id, f"任务触发重试，新任务={cloned.id}")
    await db.commit()
    queue_status = await _enqueue_after_commit(cloned)
    return {
        "taskId": str(cloned.id),
        "sourceTaskId": str(source.id),
        "status": cloned.status,
        "queueStatus": queue_status,
    }


async def _enqueue_after_commit(task: WindTaskRecord) -> str:
    """Enqueue a committed task without hiding the durable MySQL record."""
    try:
        enqueued = await TaskQueue(get_redis()).enqueue(task.id, task.priority, task.created_on)
    except RedisClientError:
        logger.exception("任务已写入 MySQL，但进入 Redis 调度池失败：task_id=%s", task.id)
        return "PENDING_QUEUE_RETRY"
    return "ENQUEUED" if enqueued else "ALREADY_SCHEDULED"


async def _remove_from_queue(task_id: int) -> None:
    try:
        await TaskQueue(get_redis()).ack(task_id)
    except RedisClientError:
        logger.exception("任务取消成功，但清理 Redis 调度池失败：task_id=%s", task_id)


async def _get_task(db: AsyncSession, task_id: int) -> WindTaskRecord:
    """
    根据任务编号查询任务记录，不存在时抛出任务不存在异常。
    """
    task = await db.scalar(select(WindTaskRecord).where(WindTaskRecord.id == task_id))
    if not task:
        raise TaskNotFoundError()
    return task


async def _get_task_def(db: AsyncSession, label: str) -> WindTaskDef:
    """
    根据模板标签查询已启用的任务模板，不存在时抛出模板异常。
    """
    task_def = await db.scalar(select(WindTaskDef).where(WindTaskDef.label == label, WindTaskDef.if_enable == 1))
    if not task_def:
        raise TemplateNotFoundError("模板不存在或未启用")
    return task_def


async def _get_available_site(db: AsyncSession, site_id: str) -> WorkSite:
    """
    查询可用站点，并校验站点存在且没有被禁用。
    """
    site = await db.scalar(select(WorkSite).where(WorkSite.site_id == site_id, WorkSite.del_ == 0))
    if not site:
        raise SiteNotFoundError(f"站点不存在: {site_id}")
    if site.disabled == 1:
        raise ResourceUnavailableError(f"站点不可用: {site_id}")
    return site


async def _get_robot(db: AsyncSession, agv_id: str) -> RobotItem:
    """
    查询可参与任务的机器人，并校验机器人存在且处于启用状态。
    """
    robot = await db.scalar(select(RobotItem).where(RobotItem.uuid == agv_id, RobotItem.del_ == 0))
    if not robot:
        raise RobotNotFoundError(f"机器人不存在: {agv_id}")
    if robot.enable_status != 1:
        raise ResourceUnavailableError(f"机器人不可接单: {agv_id}")
    return robot


def _task_target_site(task: WindTaskRecord) -> str | None:
    """
    从任务输入参数中提取任务目标站点编号。
    """
    payload = from_json_text(task.input_params, {})
    return payload.get("to")
