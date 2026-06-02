"""监控/调试接口。"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.services.monitor_service import get_system_state

router = APIRouter(tags=["监控"])


@router.get("/state")
async def system_state():
    """查询系统当前状态快照。"""
    try:
        return await get_system_state()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"状态查询失败: {str(e)}")
