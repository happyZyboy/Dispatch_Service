"""系统状态监控服务。"""

from __future__ import annotations

from typing import Any, Dict

from sqlalchemy import func, select

from app.db.models import AgvTaskORM, AmrVehicleStatusORM
from app.db.session import AsyncSessionLocal
from app.services.amr_service import get_available_amrs
from app.services.heartbeat_service import ensure_runtime_tables


async def get_system_state() -> Dict[str, Any]:
    """返回当前 AMR、任务数量的调试快照。"""
    await ensure_runtime_tables()

    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(select(AmrVehicleStatusORM))
            all_amrs = [row.to_dict() for row in result.scalars().all()]
            available = await get_available_amrs()

            # 任务统计用于调试页面或监控面板快速查看系统负载。
            pending_result = await db.execute(
                select(func.count(AgvTaskORM.task_id)).where(AgvTaskORM.task_status == 1)
            )
            running_result = await db.execute(
                select(func.count(AgvTaskORM.task_id)).where(AgvTaskORM.task_status == 2)
            )
            finished_result = await db.execute(
                select(func.count(AgvTaskORM.task_id)).where(AgvTaskORM.task_status == 3)
            )

            return {
                "total_amrs": len(all_amrs),
                "available_amrs": len(available),
                "unavailable_amrs": len(all_amrs) - len(available),
                "amr_details": all_amrs,
                "pending_tasks": pending_result.scalar() or 0,
                "running_tasks": running_result.scalar() or 0,
                "finished_tasks": finished_result.scalar() or 0,
            }
        except Exception:
            await db.rollback()
            raise
