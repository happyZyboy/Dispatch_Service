"""调度接口。"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ScheduleResponse
from app.services.scheduler_service import run_schedule

router = APIRouter(tags=["调度核心"])


@router.post("/schedule", response_model=ScheduleResponse)
async def schedule_tasks(
    battery_threshold: float = Query(0.2, description="电量最低阈值, 默认 20%"),
):
    """手动触发一轮调度。"""
    try:
        result = await run_schedule(battery_threshold=battery_threshold)
        return ScheduleResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"调度执行失败: {str(e)}")
