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
    接收调度触发请求，并执行任务选择、机器人选择和任务下发。
    """
    return success(await trigger_dispatch(db, payload.taskId, payload.agvId, payload.force))
