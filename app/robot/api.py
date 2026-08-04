from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.robot.enums import EnableStatus
from app.robot.schema import (
    RobotBatteryThresholdRequest,
    RobotHeartbeatRequest,
    RobotHistoryStateListQuery,
    RobotCurrentStateListQuery,
)
from app.robot.service import (
    heartbeat,
    list_robot_states,
    robot_history,
    set_battery_threshold,
    set_robot_enable_status,
)
from common.response.response_schema import success
from database.db import get_db

router = APIRouter(prefix="/api/v1/robots", tags=["robots"])


@router.get("/current")
async def current_states(
    payload: RobotCurrentStateListQuery,
    db: AsyncSession = Depends(get_db)
):
    """
    分页查询机器人当前状态，并支持按调度状态和机器人编号筛选。
    """
    # 当前态接口给调度器和监控页共用，重点看快照而不是历史流水。
    return success(
        await list_robot_states(
            db=db,
            page=payload.page,
            page_size=payload.page_size,
            dispatch_status=payload.dispatch_status,
            uuid=payload.uuid

        )
    )


@router.get("/{uuid}/history")
async def history(
    payload: RobotHistoryStateListQuery,
    db: AsyncSession = Depends(get_db)
):
    """
    使用游标分页查询指定机器人的历史状态变化记录。
    """

    # 历史接口回放状态变化，用来排查机器人状态抖动和切换轨迹。
    return success(
        await robot_history(
            db=db,
            uuid=payload.uuid,
            page_size=payload.page_size,
            last_started_on=payload.last_started_on,
            last_id=payload.last_id,
        )
    )


@router.post("/heartbeat")
async def report_heartbeat(payload: RobotHeartbeatRequest, db: AsyncSession = Depends(get_db)):
    """
    接收机器人心跳，更新机器人档案、当前状态快照和状态历史流水。
    """
    # 心跳上报同时完成档案补全、状态快照更新和状态流水追加。
    return success(await heartbeat(db, payload))


@router.post("/{uuid}/disable")
async def disable_robot(uuid: str, db: AsyncSession = Depends(get_db)):
    """禁用机器人，禁止其参与后续自动调度。"""
    return success(
        await set_robot_enable_status(db, uuid, EnableStatus.DISABLED),
        message="机器人已禁用",
    )


@router.post("/{uuid}/enable")
async def enable_robot(uuid: str, db: AsyncSession = Depends(get_db)):
    """启用机器人，使其可以参与自动调度。"""
    return success(
        await set_robot_enable_status(db, uuid, EnableStatus.ENABLED),
        message="机器人已启用",
    )


@router.post("/{uuid}/battery-threshold")
async def update_battery_threshold(
    uuid: str,
    payload: RobotBatteryThresholdRequest,
    db: AsyncSession = Depends(get_db),
):
    """设置机器人参与自动调度的最低电量阈值。"""
    return success(
        await set_battery_threshold(db, uuid, payload.batteryThreshold),
        message="最低调度电量阈值已更新",
    )
