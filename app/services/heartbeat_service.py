"""AMR 心跳处理服务。

这一层负责把 AMR/Robokit 上报的状态数据整理后写入数据库。
注意：车辆业务编号统一使用 equipment_info_code，对应 AMR 小车编号。
"""

from __future__ import annotations

from typing import Any, Dict

from app.db.dao.equipment import AmrVehicleStatusDAO, EquipmentInfoDAO
from app.db.models import AmrVehicleStatusORM
from app.db.session import AsyncSessionLocal, engine


async def ensure_runtime_tables() -> None:
    """确保运行时状态表存在。

    目前 amr_vehicle_status 是模拟流程必需表。若数据库脚本已经建表，
    checkfirst=True 会让这里什么也不做。
    """
    async with engine.begin() as conn:
        await conn.run_sync(AmrVehicleStatusORM.__table__.create, checkfirst=True)


def normalize_heartbeat_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """兼容 AMR 原始字段名和数据库字段名。

    Robokit 原始字段里 time/total_time 的语义是运行时长，数据库里使用
    run_time_ms/total_time_ms，让字段名直接带单位，后续维护更清楚。
    """
    normalized = dict(data)
    if normalized.get("time") is not None and normalized.get("run_time_ms") is None:
        normalized["run_time_ms"] = normalized["time"]
    if normalized.get("total_time") is not None and normalized.get("total_time_ms") is None:
        normalized["total_time_ms"] = normalized["total_time"]
    normalized.pop("time", None)
    normalized.pop("total_time", None)
    return normalized


async def upsert_equipment_status(data: Dict[str, Any]) -> Dict[str, Any]:
    """写入或更新一台 AMR 的实时状态与基础设备信息。

    一次心跳会同步两张表：
    - amr_vehicle_status：实时位置、电量、故障等高频状态
    - wms_equipment_info：设备基础档案和调度占用状态
    """
    await ensure_runtime_tables()
    data = normalize_heartbeat_data(data)
    if not data.get("equipment_info_code"):
        raise ValueError("equipment_info_code 是必填字段")

    async with AsyncSessionLocal() as db:
        try:
            # 先更新实时状态，再同步设备基础表，保证两张表使用同一个车辆编码。
            status = await AmrVehicleStatusDAO(db).upsert_by_code(data)
            equipment = await EquipmentInfoDAO(db).upsert_from_status(data)

            await db.flush()
            await db.refresh(status)
            await db.refresh(equipment)
            await db.commit()

            # 返回时合并两张表的关键字段，方便 API 层直接响应。
            row = status.to_dict()
            row.update(
                equipment_info_id=equipment.equipment_info_id,
                equipment_info_code=equipment.equipment_info_code,
                equipment_info_name=equipment.equipment_info_name,
                equipment_ip_addr=equipment.equipment_ip_addr,
            )
            return row
        except Exception:
            await db.rollback()
            raise
