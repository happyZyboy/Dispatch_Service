"""AMR 状态接口。

router 层只负责 HTTP 请求/响应，不直接写数据库。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.schemas import AmrHeartbeatRequest, AmrStatusResponse, AvailableAmrInfo, AvailableAmrResponse
from app.services.amr_service import get_available_amrs
from app.services.heartbeat_service import upsert_equipment_status

router = APIRouter(prefix="/amr", tags=["AMR状态"])


@router.post("/heartbeat", response_model=AmrStatusResponse)
async def amr_heartbeat(payload: AmrHeartbeatRequest):
    """接收 AMR 心跳状态上报。"""
    try:
        result = await upsert_equipment_status(payload.model_dump())
        return AmrStatusResponse(
            success=True,
            message="状态已更新",
            equipment_info_code=result["equipment_info_code"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"状态更新失败: {str(e)}")


@router.get("/available", response_model=AvailableAmrResponse)
async def list_available_amrs(
    battery_threshold: float = Query(0.2, description="电量最低阈值, 默认 0.2 = 20%"),
):
    """查询当前可参与调度的 AMR。"""
    try:
        amrs = await get_available_amrs(battery_threshold=battery_threshold)
        return AvailableAmrResponse(
            total=len(amrs),
            available=[
                AvailableAmrInfo(
                    equipment_info_code=a["equipment_info_code"],
                    equipment_info_name=a.get("equipment_info_name"),
                    current_station=a.get("current_station"),
                    battery_level=a.get("battery_level"),
                    blocked=bool(a.get("blocked", 0)),
                    has_err=bool(a.get("has_err", 0)),
                    charging=bool(a.get("charging", 0)),
                    last_heartbeat_time=a.get("last_heartbeat_time"),
                )
                for a in amrs
            ],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")
