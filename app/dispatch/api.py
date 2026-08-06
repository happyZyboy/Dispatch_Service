from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.dispatch.schema import DispatchTriggerRequest
from app.dispatch.service import trigger_dispatch
from common.response.response_schema import success
from database.db import get_db

router = APIRouter(prefix="/api/v1/dispatch", tags=["调度"])


@router.post("/trigger")
async def dispatch(payload: DispatchTriggerRequest, db: AsyncSession = Depends(get_db)):
    """
    接收调度触发请求，并执行任务选择、机器人选择、路径规划和任务下发。

    :param payload: 调度触发请求参数，包括任务、机器人和强制调度标记。
    :param db: 当前请求使用的异步数据库会话。
    :return: 调度分配结果和 RabbitMQ 投递状态。
    """
    return success(await trigger_dispatch(db, payload.taskId, payload.agvId, payload.force))
