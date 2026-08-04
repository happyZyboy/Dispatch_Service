from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.system.service import health_snapshot
from common.response.response_schema import success
from database.db import get_db

router = APIRouter(tags=["system"])


@router.get("/health")
@router.get("/api/v1/health")
async def health(db: AsyncSession = Depends(get_db)):
    """
    返回服务健康状态以及任务、机器人和站点数量等运行摘要。
    """
    # 健康检查同时带一点运行态摘要，便于联调时确认数据库已连通。
    return success(await health_snapshot(db))
