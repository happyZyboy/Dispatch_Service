from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.utils import format_dt, now
from database.models import MapNode, RobotCurrentState, WindTaskRecord


async def health_snapshot(db: AsyncSession) -> dict:
    """
    查询系统基础运行指标，并组装成健康检查接口使用的数据结构。
    """
    return {
        "status": "ok",
        "serverTime": format_dt(now()),
        "taskCount": await db.scalar(select(func.count()).select_from(WindTaskRecord)) or 0,
        "robotCount": await db.scalar(select(func.count()).select_from(RobotCurrentState)) or 0,
        "nodeCount": await db.scalar(select(func.count()).select_from(MapNode)) or 0,
    }
