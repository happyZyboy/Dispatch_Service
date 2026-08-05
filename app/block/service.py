from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import create_next_root_block, create_task_log, mark_robot_idle, refresh_alarm_snapshot, task_requested_sites, update_task_progress
from common.enums.block_status import BlockStatus
from common.enums.task_status import TaskStatus
from common.exception.base import InvalidRmfCallbackError, TaskNotFoundError
from common.utils import from_json_text, now, to_json_text
from database.models import WindBlockRecord, WindTaskRecord, WorkSite
from plugin.rmf.client import RmfClient


rmf_client = RmfClient()


async def handle_block_result(db: AsyncSession, payload) -> dict:
    """
    处理 RMF 的流程块结果回调，将流程块标记为运行中并记录事件。
    """
    # RMF 回调结果：先把块状态切到运行中，表示这段开始被接管。
    task = await _get_task(db, payload.taskRecordId)
    block = await _locate_block(db, payload)
    block.status = BlockStatus.RUNNING
    block.started_on = block.started_on or now()
    root = await _get_parent_root(db, block)
    if root and root.status == BlockStatus.CREATED:
        root.status = BlockStatus.RUNNING
        root.started_on = root.started_on or block.started_on
    if payload.orderId:
        block.order_id = payload.orderId
    task.status = TaskStatus.DISPATCHED
    create_task_log(db, task.id, payload.message or f"流程块 {block.block_id} 下发成功", task_block_id=block.id)
    rmf_client.report_progress({"taskId": task.id, "blockId": block.block_id, "event": "result"})
    await db.commit()
    return {"taskId": str(task.id), "blockId": block.block_id, "status": block.status}


async def handle_block_progress(db: AsyncSession, payload) -> dict:
    """
    处理 RMF 的流程块进度回调，更新流程块过程数据和任务执行状态。
    """
    # 进度回调只更新当前块的过程值，不直接结束任务。
    task = await _get_task(db, payload.taskRecordId)
    block = await _locate_block(db, payload)
    block.status = BlockStatus.RUNNING
    block.started_on = block.started_on or now()
    root = await _get_parent_root(db, block)
    if root and root.status == BlockStatus.CREATED:
        root.status = BlockStatus.RUNNING
        root.started_on = root.started_on or block.started_on
    progress = from_json_text(block.output_params, {})
    progress["progress"] = payload.progress
    progress.update(payload.detail)
    block.output_params = to_json_text(progress)
    if task.first_executor_time is None:
        task.first_executor_time = now()
    task.status = TaskStatus.EXECUTING
    create_task_log(db, task.id, payload.message or f"流程块 {block.block_id} 执行中", task_block_id=block.id)
    rmf_client.report_progress({"taskId": task.id, "blockId": block.block_id, "progress": payload.progress})
    await db.commit()
    return {"taskId": str(task.id), "blockId": block.block_id, "status": block.status, "progress": payload.progress}


async def handle_block_complete(db: AsyncSession, payload) -> dict:
    """
    处理单个流程块完成回调，并在全部动作块完成后结束整项任务。
    """
    # 单个动作块完成后，只检查它所属的 RootBp；整条任务的下一个 RootBp
    # 必须等当前 RootBp 完成并收到 RMF 到达回调后再创建。
    task = await _get_task(db, payload.taskRecordId)
    block = await _locate_block(db, payload)
    if block.status == BlockStatus.SUCCESS:
        return {"taskId": str(task.id), "blockId": block.block_id, "status": block.status, "taskStatus": task.status}
    block.status = BlockStatus.SUCCESS
    block.started_on = block.started_on or now()
    block.ended_on = now()
    output = from_json_text(block.output_params, {})
    output.update(payload.detail)
    block.output_params = to_json_text(output)
    create_task_log(db, task.id, payload.message or f"流程块 {block.block_id} 执行完成", task_block_id=block.id)

    root = await _get_parent_root(db, block)
    if root is None:
        raise InvalidRmfCallbackError("RMF 回调数据非法：动作块没有所属 RootBp")

    remaining = (
        await db.scalars(
            select(WindBlockRecord).where(
                WindBlockRecord.task_record_id == task.id,
                WindBlockRecord.parent_block_id == root.block_id,
                WindBlockRecord.block_name == "CAgvOperationBp",
                WindBlockRecord.status != BlockStatus.SUCCESS,
            )
        )
    ).all()
    if remaining:
        task.status = TaskStatus.EXECUTING
        await update_task_progress(db, task)
        rmf_client.report_complete({"taskId": task.id, "blockId": block.block_id})
        await db.commit()
        return {"taskId": str(task.id), "blockId": block.block_id, "status": block.status, "taskStatus": task.status}

    root.status = BlockStatus.SUCCESS
    root.started_on = root.started_on or block.started_on or now()
    root.ended_on = now()
    root.output_params = to_json_text({"arrived": True, "currentSite": _block_to_site(block)})
    root_variables = from_json_text(root.internal_variables, {})
    target_index = int(root_variables.get("targetIndex") or 0)
    root_variables["arrived"] = True
    root.internal_variables = to_json_text(root_variables)
    await update_task_progress(db, task)
    rmf_client.report_complete({"taskId": task.id, "blockId": block.block_id, "rootBlockId": root.block_id})

    requested_sites = task_requested_sites(task)
    next_target_index = target_index + 1
    if next_target_index >= len(requested_sites):
        await _finish_task(db, task)
        await db.commit()
        return {"taskId": str(task.id), "blockId": block.block_id, "rootBlockId": root.block_id, "status": block.status, "taskStatus": task.status}

    arrived_site = _block_to_site(block) or requested_sites[target_index]
    next_blocks = await create_next_root_block(db, task, arrived_site, next_target_index)
    task.status = TaskStatus.ASSIGNED
    variables = from_json_text(task.variables, {})
    variables["currentSite"] = arrived_site
    variables["nextSite"] = requested_sites[next_target_index]
    variables["rmfPublished"] = False
    variables.pop("rmfDispatchKey", None)
    variables.pop("rmfTaskId", None)
    task.variables = to_json_text(variables)
    create_task_log(db, task.id, f"RootBp 已完成，准备创建下一个 RootBp：{next_blocks[0].block_id}")
    await db.flush()
    await db.commit()

    # 重新复用调度入口，只发布新创建的 RootBp，不重复选车。
    from app.dispatch.service import trigger_dispatch

    dispatch_result = await trigger_dispatch(db, task.id, task.agv_id, False)
    return {
        "taskId": str(task.id),
        "blockId": block.block_id,
        "rootBlockId": root.block_id,
        "nextRootBlockId": next_blocks[0].block_id,
        "status": block.status,
        "taskStatus": task.status,
        "nextDispatch": dispatch_result,
    }


async def handle_block_failed(db: AsyncSession, payload) -> dict:
    """
    处理流程块失败回调，更新失败信息并释放相关业务资源。
    """
    # 失败回调要同步收口任务、机器人和站点占用状态。
    task = await _get_task(db, payload.taskRecordId)
    block = await _locate_block(db, payload)
    block.status = BlockStatus.FAILED
    block.started_on = block.started_on or now()
    block.ended_on = now()
    block.ended_reason = payload.message or "RMF 执行失败"
    root = await _get_parent_root(db, block)
    if root:
        root.status = BlockStatus.FAILED
        root.ended_on = now()
        root.ended_reason = block.ended_reason
    task.status = TaskStatus.FAILED
    task.ended_on = now()
    task.ended_reason = block.ended_reason
    create_task_log(db, task.id, f"流程块失败：{block.ended_reason}", level="ERROR", task_block_id=block.id)
    if task.agv_id:
        await mark_robot_idle(db, task.agv_id, _target_site(task))
        await refresh_alarm_snapshot(db, task.agv_id)
    await _release_task_sites(db, task, success=False)
    rmf_client.report_failed({"taskId": task.id, "blockId": block.block_id, "reason": block.ended_reason})
    await db.commit()
    return {"taskId": str(task.id), "blockId": block.block_id, "status": block.status, "taskStatus": task.status}


async def _finish_task(db: AsyncSession, task: WindTaskRecord) -> None:
    """
    对已完成全部动作块的任务执行统一收尾，包括状态、日志和资源释放。
    """
    # 所有动作块完成后再统一收尾，避免中途过早结束任务。
    task.status = TaskStatus.COMPLETED
    task.ended_on = now()
    create_task_log(db, task.id, "任务执行完成")
    if task.agv_id:
        await mark_robot_idle(db, task.agv_id, _target_site(task))
    await _release_task_sites(db, task, success=True)


async def _get_parent_root(db: AsyncSession, block: WindBlockRecord) -> WindBlockRecord | None:
    """返回拥有该动作流程块的 RootBp。"""
    if block.block_name == "RootBp":
        return block
    if not block.parent_block_id:
        return None
    return await db.scalar(
        select(WindBlockRecord).where(
            WindBlockRecord.task_record_id == block.task_record_id,
            WindBlockRecord.block_id == block.parent_block_id,
            WindBlockRecord.block_name == "RootBp",
        )
    )


def _block_to_site(block: WindBlockRecord) -> str | None:
    """提取 RMF 回调或动作流程块输入中记录的到达站点。"""
    output = from_json_text(block.output_params, {})
    if output.get("currentSite"):
        return str(output["currentSite"])
    values = from_json_text(block.block_input_params_value, {})
    return values.get("to")


async def _release_task_sites(db: AsyncSession, task: WindTaskRecord, success: bool) -> None:
    """
    根据任务起点和终点释放站点预占状态，并在成功时更新站点装载状态。
    """
    # 成功时释放占用并落终点状态，失败时只回收预占资源。
    params = from_json_text(task.input_params, {})
    path = from_json_text(task.path, {})
    route = path.get("route") or [params.get("from"), params.get("to")]
    site_ids = {site_id for site_id in route if site_id}
    sites = (await db.scalars(select(WorkSite).where(WorkSite.site_id.in_(site_ids)))).all()
    first_site = route[0] if route else None
    last_site = route[-1] if route else None
    for site in sites:
        if site.agv_id not in {None, task.agv_id}:
            continue
        site.preparing = 0
        site.agv_id = None
        site.holder = 0
        if success and site.site_id == first_site:
            site.filled = 0
        if success and site.site_id == last_site:
            site.filled = 1


def _target_site(task: WindTaskRecord) -> str | None:
    """
    从任务输入参数中读取任务目标站点。
    """
    params = from_json_text(task.input_params, {})
    return params.get("to")


async def _get_task(db: AsyncSession, task_record_id: int) -> WindTaskRecord:
    """
    根据任务记录主键查询任务，不存在时抛出任务不存在异常。
    """
    task = await db.scalar(select(WindTaskRecord).where(WindTaskRecord.id == task_record_id))
    if not task:
        raise TaskNotFoundError()
    return task


async def _locate_block(db: AsyncSession, payload) -> WindBlockRecord:
    """
    根据 RMF 回调中的流程块标识定位流程块，必要时回退到首个待处理动作块。
    """
    # 优先按明确标识定位块，找不到时回退到当前任务的首个待处理动作块。
    stmt = select(WindBlockRecord).where(WindBlockRecord.task_record_id == payload.taskRecordId)
    if payload.blockId:
        stmt = stmt.where(WindBlockRecord.block_id == payload.blockId)
    elif payload.orderId:
        stmt = stmt.where(WindBlockRecord.order_id == payload.orderId)
    elif payload.blockName:
        stmt = stmt.where(WindBlockRecord.block_name == payload.blockName)
    block = await db.scalar(stmt.order_by(WindBlockRecord.id.asc()))
    if block:
        return block
    fallback = await db.scalar(
        select(WindBlockRecord)
        .where(
            WindBlockRecord.task_record_id == payload.taskRecordId,
            WindBlockRecord.block_name == "CAgvOperationBp",
            WindBlockRecord.status.in_([BlockStatus.CREATED, BlockStatus.RUNNING]),
        )
        .order_by(WindBlockRecord.id.asc())
    )
    if not fallback:
        raise InvalidRmfCallbackError("RMF 回调数据非法：未匹配到流程块")
    return fallback
