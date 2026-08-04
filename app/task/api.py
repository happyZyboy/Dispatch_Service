from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.task.schema import TaskCancelRequest, TaskRetryRequest, TaskSubmitRequest
from app.task.service import cancel_task, get_task_detail, list_tasks, retry_task, submit_task
from common.response.response_schema import success
from database.db import get_db

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])


@router.post("/submit")
async def submit(payload: TaskSubmitRequest, db: AsyncSession = Depends(get_db)):
    """
    接收任务提交请求，创建任务记录并返回任务初始快照。
    """
    # 提交任务接口对应文档里的“提交任务”，返回的是初版任务快照。
    return success(
        await submit_task(
            db=db,
            template_label=payload.templateLabel,
            site_path=payload.sitePath,
            priority=payload.priority,
            agv_id=payload.agvId,
            out_order_no=payload.outOrderNo,
            periodic_task=payload.periodicTask,
            remark=payload.remark,
        )
    )


@router.get("/{task_id}")
async def detail(task_id: int, db: AsyncSession = Depends(get_db)):
    """
    查询指定任务的详情，并同时返回流程块和任务日志。
    """
    # 详情接口把任务、流程块和日志一次性带回，前端不需要自己拼装。
    return success(await get_task_detail(db, task_id))


@router.get("")
async def listing(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100, alias="pageSize"),
    status: int | None = None,
    agv_id: str | None = Query(default=None, alias="agvId"),
    from_site: str | None = Query(default=None, alias="fromSite"),
    to_site: str | None = Query(default=None, alias="toSite"),
    keyword: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """
    按状态、机器人、起点、终点和关键字分页查询任务列表。
    """
    # 分页列表主要用于任务台账和任务追踪页。
    return success(await list_tasks(db, page, page_size, status, agv_id, from_site, to_site, keyword))


@router.post("/{task_id}/cancel")
async def cancel(task_id: int, payload: TaskCancelRequest, db: AsyncSession = Depends(get_db)):
    """
    取消指定任务，并同步更新任务结束信息和机器人资源状态。
    """
    # 取消任务会同步收口任务状态，并回收相关资源占用。
    return success(await cancel_task(db, task_id, payload.reason))


@router.post("/{task_id}/retry")
async def retry(task_id: int, payload: TaskRetryRequest, db: AsyncSession = Depends(get_db)):
    """
    重试指定任务，通过复制源任务创建一条新的待调度任务记录。
    """
    # 重试接口不是原地回滚，而是创建一条新的任务实例。
    return success(await retry_task(db, task_id, payload.reason))
