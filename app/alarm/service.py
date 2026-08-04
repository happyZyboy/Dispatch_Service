from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import refresh_alarm_snapshot
from common.exception.base import AlarmNotFoundError
from common.utils import now, paginate
from database.models import AlarmRecord, RobotCurrentState


async def report_alarm(db: AsyncSession, vehicle_id: str, alarms_code: str, alarms_desc: str, level: str, type_: int) -> dict:
    """
    保存一条新的报警记录，并同步更新对应机器人的报警状态快照。
    """
    alarm = AlarmRecord(
        vehicle_id=vehicle_id,
        alarms_code=alarms_code,
        alarms_desc=alarms_desc,
        level=level,
        type_=type_,
    )
    db.add(alarm)
    await db.flush()
    state = await db.scalar(select(RobotCurrentState).where(RobotCurrentState.uuid == vehicle_id))
    if state:
        state.has_unresolved_alarm = 1
        state.alarm_level = level
        state.updated_at = now()
    await db.commit()
    return alarm.to_dict()


async def recover_alarm(db: AsyncSession, alarm_id: int, reason: str | None) -> dict:
    """
    结束指定报警记录，计算报警持续时间并刷新机器人报警汇总状态。
    """
    alarm = await db.scalar(select(AlarmRecord).where(AlarmRecord.id == alarm_id))
    if not alarm:
        raise AlarmNotFoundError()
    alarm.ended_on = now()
    alarm.alarms_cost_time = float((alarm.ended_on - alarm.started_on).total_seconds())
    if reason:
        alarm.alarms_desc = f"{alarm.alarms_desc} | recoverReason={reason}"
    await refresh_alarm_snapshot(db, alarm.vehicle_id)
    await db.commit()
    return alarm.to_dict()


async def list_alarms(db: AsyncSession, page: int, page_size: int, vehicle_id: str | None, level: str | None, unresolved_only: bool) -> dict:
    """
    查询报警记录，并根据筛选条件和分页参数返回报警列表。
    """
    alarms = (await db.scalars(select(AlarmRecord).order_by(AlarmRecord.started_on.desc()))).all()
    items = []
    for alarm in alarms:
        if vehicle_id and alarm.vehicle_id != vehicle_id:
            continue
        if level and alarm.level != level:
            continue
        if unresolved_only and alarm.ended_on is not None:
            continue
        items.append(alarm.to_dict())
    return paginate(items, page, page_size)
