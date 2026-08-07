from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from sqlalchemy import select

from common.enums.dispatch_status import DispatchStatus
from common.enums.robot_status import RobotStatus
from common.utils import now, robot_location_json
from core.conf import settings
from database.db import SessionLocal
from database.models import RobotCurrentState, RobotStatusRecord


logger = logging.getLogger(__name__)


async def scan_stale_robot_heartbeats() -> int:
    """将超过心跳超时时间的机器人标记为离线，并记录状态流水。"""
    cutoff = now() - timedelta(seconds=settings.robot_heartbeat_timeout_seconds)

    async with SessionLocal() as db:
        states = (
            await db.scalars(
                select(RobotCurrentState).where(
                    RobotCurrentState.last_heartbeat_at.is_not(None),
                    RobotCurrentState.last_heartbeat_at < cutoff,
                    RobotCurrentState.dispatch_status != DispatchStatus.OFFLINE,
                )
            )
        ).all()

        if not states:
            return 0

        transitioned_at = now()
        try:
            for state in states:
                if state.last_status_record_id is not None:
                    last_record = await db.scalar(
                        select(RobotStatusRecord).where(
                            RobotStatusRecord.id == state.last_status_record_id
                        )
                    )
                    if last_record and last_record.ended_on is None:
                        last_record.ended_on = transitioned_at
                        last_record.duration = int(
                            (transitioned_at - last_record.started_on).total_seconds()
                        )

                offline_record = RobotStatusRecord(
                    uuid=state.uuid,
                    vehicle_name=state.vehicle_name,
                    old_status=state.current_status,
                    new_status=RobotStatus.OFFLINE,
                    location=robot_location_json(state.current_x, state.current_y),
                )
                db.add(offline_record)
                await db.flush()

                state.current_status = RobotStatus.OFFLINE
                state.dispatch_status = DispatchStatus.OFFLINE
                state.last_status_record_id = offline_record.id
                state.updated_at = transitioned_at

            await db.commit()
        except Exception:
            await db.rollback()
            raise

        return len(states)


async def run_robot_heartbeat_monitor(stop_event: asyncio.Event) -> None:
    """按固定间隔巡检机器人心跳，直到应用关闭。"""
    interval = max(1, settings.robot_heartbeat_scan_interval_seconds)

    while not stop_event.is_set():
        try:
            offline_count = await scan_stale_robot_heartbeats()
            if offline_count:
                logger.info("心跳监控：%s 个机器人被标记为离线", offline_count)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("心跳监控失败")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue
