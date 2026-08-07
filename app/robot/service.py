from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.exception.base import RobotNotFoundError, ResourceUnavailableError
from common.utils import format_dt, now, paginate, robot_location_json
from database.models import RobotCurrentState, RobotItem, RobotStatusRecord
from app.robot.enums import EnableStatus


async def list_robot_states(
    db: AsyncSession,
    page: int,
    page_size: int,
    dispatch_status: int | None,
    uuid: str | None,
) -> dict:
    """
    查询机器人档案和当前状态，并按条件返回分页后的机器人状态列表。
    """
    robots = (await db.scalars(select(RobotItem).where(RobotItem.del_ == 0).order_by(RobotItem.added_on.asc()))).all()
    states = {row.uuid: row for row in (await db.scalars(select(RobotCurrentState))).all()}
    items = []
    for robot in robots:
        state = states.get(robot.uuid)
        merged = robot.to_dict()
        if state:
            merged.update(state.to_dict())
        if dispatch_status is not None and merged.get("dispatch_status") != dispatch_status:
            continue
        if uuid and merged.get("uuid") != uuid:
            continue
        items.append(
            {
                "uuid": merged.get("uuid"),
                "vehicleName": merged.get("vehicle_name") or merged.get("robot_name"),
                "currentStatus": merged.get("current_status"),
                "dispatchStatus": merged.get("dispatch_status"),
                "currentTaskId": merged.get("current_task_id"),
                "currentSiteId": merged.get("current_site_id"),
                "currentX": merged.get("current_x"),
                "currentY": merged.get("current_y"),
                "batteryLevel": merged.get("battery_level"),
                "hasUnresolvedAlarm": merged.get("has_unresolved_alarm"),
                "alarmLevel": merged.get("alarm_level"),
                "lastHeartbeatAt": merged.get("last_heartbeat_at"),
            }
        )
    return paginate(items, page, page_size)


async def robot_history(
    db: AsyncSession,
    uuid: str,
    page_size: int,
    last_started_on: datetime | None = None,
    last_id: int | None = None,
) -> dict:
    """
    查询指定机器人的状态历史记录，并使用开始时间和主键进行游标分页。
    """
    robot = await db.scalar(select(RobotItem).where(RobotItem.uuid == uuid))
    if not robot:
        raise RobotNotFoundError()

    conditions = [RobotStatusRecord.uuid == uuid]

    # 历史记录按时间倒序返回，游标之后继续查询更早的记录。
    if last_started_on is not None and last_id is not None:
        conditions.append(
            or_(
                RobotStatusRecord.started_on < last_started_on,
                and_(
                    RobotStatusRecord.started_on == last_started_on,
                    RobotStatusRecord.id < last_id,
                ),
            )
        )

    stmt = (
        select(RobotStatusRecord)
        .where(*conditions)
        .order_by(RobotStatusRecord.started_on.desc(), RobotStatusRecord.id.desc())
        .limit(page_size + 1)
    )
    rows = (await db.scalars(stmt)).all()
    has_next = len(rows) > page_size
    rows = rows[:page_size]

    items = [item.to_dict() for item in rows]
    next_cursor = None
    if has_next and rows:
        last_row = rows[-1]
        next_cursor = {
            "lastStartedOn": format_dt(last_row.started_on),
            "lastId": last_row.id,
        }

    total = await db.scalar(
        select(func.count()).select_from(RobotStatusRecord).where(RobotStatusRecord.uuid == uuid)
    ) or 0

    return {
        "pageSize": page_size,
        "total": total,
        "hasNext": has_next,
        "nextCursor": next_cursor,
        "items": items,
    }


async def heartbeat(db: AsyncSession, payload) -> dict:
    """
    处理机器人心跳数据，维护机器人档案、当前快照和状态变化流水。
    """
    robot = await db.scalar(select(RobotItem).where(RobotItem.uuid == payload.uuid))
    if not robot:
        raise RobotNotFoundError()

    robot.robot_name = payload.vehicleName or robot.robot_name
    robot.robot_type = payload.robotType or robot.robot_type
    robot.current_map = payload.currentMap or robot.current_map

    state = await db.scalar(select(RobotCurrentState).where(RobotCurrentState.uuid == payload.uuid))
    previous_status = None
    if not state:
        state = RobotCurrentState(
            robot_id=robot.id,
            uuid=payload.uuid,
            vehicle_name=payload.vehicleName or robot.robot_name
        )
        db.add(state)
        await db.flush()
    else:
        previous_status = state.current_status

    last_record = await db.scalar(select(RobotStatusRecord).where(RobotStatusRecord.id == state.last_status_record_id))
    if last_record and previous_status is not None and previous_status != payload.currentStatus and last_record.ended_on is None:
        last_record.ended_on = now()
        last_record.duration = int((last_record.ended_on - last_record.started_on).total_seconds())

    record = RobotStatusRecord(
        uuid=payload.uuid,
        vehicle_name=payload.vehicleName or robot.robot_name,
        old_status=previous_status,
        new_status=payload.currentStatus,
        location=robot_location_json(payload.currentX, payload.currentY),
        odo=payload.odo,
        today_odo=payload.todayOdo
    )
    db.add(record)
    await db.flush()

    state.vehicle_name = payload.vehicleName or robot.robot_name
    state.current_status = payload.currentStatus
    state.dispatch_status = payload.dispatchStatus
    state.current_task_id = payload.currentTaskId
    state.current_site_id = payload.currentSiteId
    state.current_x = payload.currentX
    state.current_y = payload.currentY
    state.battery_level = payload.batteryLevel
    state.has_unresolved_alarm = payload.hasUnresolvedAlarm
    state.alarm_level = payload.alarmLevel
    state.last_status_record_id = record.id
    state.last_heartbeat_at = now()
    state.updated_at = now()
    await db.commit()
    return state.to_dict()


async def set_robot_enable_status(db: AsyncSession, uuid: str, status: EnableStatus) -> dict:
    """设置机器人启用状态，禁用只影响后续自动调度。"""
    robot = await db.scalar(
        select(RobotItem).where(RobotItem.uuid == uuid, RobotItem.del_ == 0)
    )
    if not robot:
        raise RobotNotFoundError()

    robot.enable_status = int(status)
    await db.commit()
    await db.refresh(robot)
    return {
        "uuid": robot.uuid,
        "enableStatus": robot.enable_status,
        "batteryThreshold": float(robot.battery_threshold) if robot.battery_threshold is not None else None,
    }


async def set_battery_threshold(db: AsyncSession, uuid: str, threshold: float) -> dict:
    """设置机器人参与自动调度的最低电量阈值。"""
    robot = await db.scalar(
        select(RobotItem).where(RobotItem.uuid == uuid, RobotItem.del_ == 0)
    )
    if not robot:
        raise RobotNotFoundError()

    robot.battery_threshold = threshold
    await db.commit()
    await db.refresh(robot)
    return {
        "uuid": robot.uuid,
        "enableStatus": robot.enable_status,
        "batteryThreshold": float(robot.battery_threshold) if robot.battery_threshold is not None else None,
    }
