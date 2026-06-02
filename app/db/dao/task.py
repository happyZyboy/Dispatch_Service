"""任务相关 DAO。

负责 agv_task、agv_task_log、event_outbox 的数据库操作。
"""

from __future__ import annotations

from datetime import datetime
from typing import List

from sqlalchemy import select, update

from app.db.dao.base import BaseDAO
from app.db.models.task import AgvTaskLogORM, AgvTaskORM, EventOutboxORM


class AgvTaskDAO(BaseDAO):
    """agv_task 表访问对象。"""

    model = AgvTaskORM

    async def list_by_status(self, task_status: int) -> List[AgvTaskORM]:
        """按任务状态查询任务。"""
        return await self.list_by_filter(task_status=task_status)

    async def list_pending(self) -> List[AgvTaskORM]:
        """查询待分配任务，优先级高的排在前面。"""
        result = await self.db.execute(
            select(self.model)
            .where(self.model.task_status == 1)
            .order_by(self.model.priority_value.desc(), self.model.create_time.asc())
        )
        return result.scalars().all()

    async def assign_to_equipment(self, task_id: int, equipment: dict) -> int:
        """把任务绑定到指定 AMR，并标记为已分配。"""
        result = await self.db.execute(
            update(self.model)
            .where(self.model.task_id == task_id)
            .values(
                agv_code=equipment["equipment_info_code"],
                equipment_info_id=equipment.get("equipment_info_id"),
                equipment_info_code=equipment["equipment_info_code"],
                equipment_info_name=equipment.get("equipment_info_name"),
                equipment_ip_addr=equipment.get("equipment_ip_addr") or equipment.get("current_ip"),
                task_status=2,
                task_detail_status=3,
                modified_time=datetime.now(),
            )
        )
        await self.db.flush()
        return result.rowcount

    async def cancel_pending_demo_tasks(self) -> int:
        """取消旧的模拟待分配任务，避免多次 /demo/run 互相干扰。"""
        result = await self.db.execute(
            update(self.model)
            .where(self.model.task_code.like("SIM-%"), self.model.task_status == 1)
            .values(task_status=6, task_detail_status=601, modified_time=datetime.now())
        )
        await self.db.flush()
        return result.rowcount


class AgvTaskLogDAO(BaseDAO):
    """agv_task_log 表访问对象。"""

    model = AgvTaskLogORM

    async def write_log(self, task_id: int, log_type: str, log_content: str) -> AgvTaskLogORM:
        """写入一条任务日志。"""
        log = AgvTaskLogORM(task_id=task_id, task_log_type=log_type, log_content=log_content)
        self.db.add(log)
        await self.db.flush()
        return log


class EventOutboxDAO(BaseDAO):
    """event_outbox 表访问对象。"""

    model = EventOutboxORM

    async def mark_task_published(self, task_id: int) -> int:
        """调度完成后，把任务事件标记为已发布/已消费。"""
        result = await self.db.execute(
            update(self.model)
            .where(self.model.task_id == task_id)
            .values(state=2, status="PUBLISHED", published_at=datetime.now(), update_time=datetime.now())
        )
        await self.db.flush()
        return result.rowcount
