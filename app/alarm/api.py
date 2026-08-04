from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.alarm.schema import AlarmRecoverRequest, AlarmReportRequest
from app.alarm.service import list_alarms, recover_alarm, report_alarm
from common.response.response_schema import success
from database.db import get_db

router = APIRouter(prefix="/api/v1/alarms", tags=["alarms"])


@router.post("/report")
async def report(payload: AlarmReportRequest, db: AsyncSession = Depends(get_db)):
    """
    接收机器人或外部系统上报的报警，并返回创建后的报警记录。
    """
    # 报警上报会同时写报警记录和机器人当前告警快照。
    return success(await report_alarm(db, payload.vehicleId, payload.alarmsCode, payload.alarmsDesc, payload.level, payload.type))


@router.post("/{alarm_id}/recover")
async def recover(alarm_id: int, payload: AlarmRecoverRequest, db: AsyncSession = Depends(get_db)):
    """
    恢复指定报警，结束报警周期并刷新机器人当前报警快照。
    """
    # 恢复接口结束报警周期，并刷新机器人告警汇总状态。
    return success(await recover_alarm(db, alarm_id, payload.reason))


@router.get("")
async def listing(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100, alias="pageSize"),
    vehicle_id: str | None = Query(default=None, alias="vehicleId"),
    level: str | None = None,
    unresolved_only: bool = Query(default=False, alias="unresolvedOnly"),
    db: AsyncSession = Depends(get_db),
):
    """
    按车辆、报警级别和是否未恢复条件分页查询报警记录。
    """
    # 报警列表支持按车辆、级别和未恢复状态过滤。
    return success(await list_alarms(db, page, page_size, vehicle_id, level, unresolved_only))
