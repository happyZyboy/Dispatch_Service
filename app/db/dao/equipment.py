"""设备/AMR 相关 DAO。

DAO 只负责数据库访问，不承载调度业务决策。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import select, update

from app.db.dao.base import BaseDAO
from app.db.models.equipment import AmrVehicleStatusORM, EquipmentInfoORM, EquipmentLogORM


class EquipmentInfoDAO(BaseDAO):
    """wms_equipment_info 表访问对象。"""

    model = EquipmentInfoORM

    async def get_by_code(self, equipment_info_code: str) -> Optional[EquipmentInfoORM]:
        """按设备业务编码查询基础设备信息。"""
        result = await self.db.execute(
            select(self.model).where(self.model.equipment_info_code == equipment_info_code)
        )
        return result.scalar_one_or_none()

    async def list_online(self) -> List[EquipmentInfoORM]:
        return await self.list_by_filter(if_online=1)

    async def update_state(self, equipment_info_code: str, state: int, task_id: int | None = None) -> int:
        """更新设备调度状态。

        state: 0-空闲, 1-繁忙, 2-故障。
        """
        values = {"state": state, "update_time": datetime.now()}
        if task_id is not None:
            values["task_id"] = task_id
        result = await self.db.execute(
            update(self.model)
            .where(self.model.equipment_info_code == equipment_info_code)
            .values(**values)
        )
        await self.db.flush()
        return result.rowcount

    async def mark_idle_many(self, equipment_info_codes: List[str]) -> int:
        """批量把车辆重置为空闲，主要用于模拟流程初始化。"""
        result = await self.db.execute(
            update(self.model)
            .where(self.model.equipment_info_code.in_(equipment_info_codes))
            .values(state=0, task_id=None, work_state="demo idle", update_time=datetime.now())
        )
        await self.db.flush()
        return result.rowcount

    async def upsert_from_status(self, data: Dict[str, Any]) -> EquipmentInfoORM:
        """根据 AMR 心跳同步设备基础表。"""
        equipment_info_code = data["equipment_info_code"]
        equipment = await self.get_by_code(equipment_info_code)
        state = 2 if data.get("has_err") else 1 if data.get("blocked") or data.get("charging") else 0
        name = data.get("equipment_info_name") or equipment_info_code
        now = datetime.now()

        if equipment is None:
            equipment = EquipmentInfoORM(
                equipment_info_code=equipment_info_code,
                equipment_info_name=name,
                equipment_ip_addr=data.get("current_ip"),
                if_auto_connect_tcp=1,
                if_online=1,
                state=state,
                work_state=f"station={data.get('current_station') or ''}; battery={data.get('battery_level')}",
                update_time=now,
            )
            self.db.add(equipment)
        else:
            equipment.equipment_info_name = name
            equipment.equipment_ip_addr = data.get("current_ip") or equipment.equipment_ip_addr
            equipment.if_online = 1
            if equipment.state != 1 or equipment.task_id is None:
                equipment.state = state
            equipment.work_state = f"station={data.get('current_station') or ''}; battery={data.get('battery_level')}"
            equipment.update_time = now

        await self.db.flush()
        return equipment


class AmrVehicleStatusDAO(BaseDAO):
    """amr_vehicle_status 表访问对象。"""

    model = AmrVehicleStatusORM

    async def get_by_code(self, equipment_info_code: str) -> Optional[AmrVehicleStatusORM]:
        """按 AMR 业务编码查询实时状态。"""
        result = await self.db.execute(
            select(self.model).where(self.model.equipment_info_code == equipment_info_code)
        )
        return result.scalar_one_or_none()

    async def upsert_by_code(self, data: Dict[str, Any]) -> AmrVehicleStatusORM:
        """按 AMR 业务编码写入或更新实时状态。"""
        equipment_info_code = data["equipment_info_code"]
        existing = await self.get_by_code(equipment_info_code)
        now = datetime.now()

        update_fields = {"last_heartbeat_time": now, "update_time": now}
        for field_name, value in data.items():
            if value is None or field_name == "equipment_info_code":
                continue
            if not hasattr(self.model, field_name):
                continue
            # 数据库中布尔字段使用 tinyint 存储，这里统一转换成 0/1。
            update_fields[field_name] = 1 if isinstance(value, bool) and value else 0 if isinstance(value, bool) else value

        if existing is None:
            existing = self.model(equipment_info_code=equipment_info_code, **update_fields)
            self.db.add(existing)
        else:
            for key, value in update_fields.items():
                setattr(existing, key, value)

        await self.db.flush()
        return existing


class EquipmentLogDAO(BaseDAO):
    model = EquipmentLogORM
