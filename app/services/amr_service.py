"""AMR 查询服务。"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

from sqlalchemy import select

from app.db.dao.equipment import EquipmentInfoDAO
from app.db.models import AmrVehicleStatusORM
from app.db.session import AsyncSessionLocal
from app.services.heartbeat_service import ensure_runtime_tables

BATTERY_THRESHOLD = 0.2
HEARTBEAT_TIMEOUT_SEC = 120


async def get_available_amrs(battery_threshold: float = BATTERY_THRESHOLD) -> List[Dict[str, Any]]:
    """查询当前可调度 AMR。

    可调度条件：
    - 无阻挡、无故障、未充电
    - 电量不低于阈值
    - 心跳未超时
    - wms_equipment_info 中在线且空闲
    """
    await ensure_runtime_tables()
    cutoff_time = datetime.now() - timedelta(seconds=HEARTBEAT_TIMEOUT_SEC)

    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(
                select(AmrVehicleStatusORM).where(
                    AmrVehicleStatusORM.blocked == 0,
                    AmrVehicleStatusORM.has_err == 0,
                    AmrVehicleStatusORM.charging == 0,
                    AmrVehicleStatusORM.battery_level >= battery_threshold,
                    AmrVehicleStatusORM.last_heartbeat_time >= cutoff_time,
                )
            )
            status_rows = [row.to_dict() for row in result.scalars().all()]
            if not status_rows:
                return []

            # 实时状态表只代表小车当前状态，还需要结合设备基础表判断在线/空闲。
            equipment_codes = [row["equipment_info_code"] for row in status_rows]
            equipment_result = await db.execute(
                select(EquipmentInfoDAO.model).where(
                    EquipmentInfoDAO.model.equipment_info_code.in_(equipment_codes)
                )
            )
            equipment_by_code = {
                item.equipment_info_code: item for item in equipment_result.scalars().all()
            }

            available: List[Dict[str, Any]] = []
            for row in status_rows:
                equipment = equipment_by_code.get(row["equipment_info_code"])
                if equipment is not None:
                    # state=0 表示空闲；繁忙或故障的设备不参与本轮调度。
                    if equipment.if_online != 1 or equipment.state != 0:
                        continue
                    row.update(
                        equipment_info_id=equipment.equipment_info_id,
                        equipment_info_name=equipment.equipment_info_name,
                        equipment_ip_addr=equipment.equipment_ip_addr,
                    )
                available.append(row)
            return available
        except Exception:
            await db.rollback()
            raise
