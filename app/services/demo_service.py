"""模拟数据服务。

用于在不接入成员A/OpenRMF/真实 AMR 的情况下，先跑通：
AMR 状态上报 -> 任务入库 -> 调度分配。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from app.db.dao.equipment import EquipmentInfoDAO
from app.db.dao.task import AgvTaskDAO, AgvTaskLogDAO
from app.db.models import AgvTaskORM, EventOutboxORM
from app.db.session import AsyncSessionLocal
from app.services.heartbeat_service import ensure_runtime_tables, upsert_equipment_status
from app.services.scheduler_service import run_schedule


async def seed_demo_data() -> Dict[str, Any]:
    """写入 3 台模拟 AMR 和 3 条待调度任务。"""
    await ensure_runtime_tables()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    demo_amrs = [
        {
            "equipment_info_code": "AMR01",
            "equipment_info_name": "AMR 01号车",
            "current_ip": "192.168.10.101",
            "current_station": "A01",
            "last_station": "A00",
            "battery_level": 0.86,
            "battery_temp": 31.5,
            "charging": False,
            "blocked": False,
            "has_err": False,
            "pose_x": 1.2,
            "pose_y": 3.4,
            "confidence": 0.97,
        },
        {
            "equipment_info_code": "AMR02",
            "equipment_info_name": "AMR 02号车",
            "current_ip": "192.168.10.102",
            "current_station": "B01",
            "last_station": "B00",
            "battery_level": 0.62,
            "battery_temp": 32.0,
            "charging": False,
            "blocked": False,
            "has_err": False,
            "pose_x": 4.2,
            "pose_y": 2.1,
            "confidence": 0.95,
        },
        {
            "equipment_info_code": "AMR03",
            "equipment_info_name": "AMR 03号车",
            "current_ip": "192.168.10.103",
            "current_station": "C01",
            "last_station": "C00",
            "battery_level": 0.18,
            "battery_temp": 30.0,
            "charging": False,
            "blocked": False,
            "has_err": False,
            "pose_x": 8.0,
            "pose_y": 6.3,
            "confidence": 0.96,
        },
    ]

    vehicles = [await upsert_equipment_status(amr) for amr in demo_amrs]
    equipment_codes = [amr["equipment_info_code"] for amr in demo_amrs]

    # 让模拟车回到空闲状态，保证 /demo/run 可以反复执行。
    async with AsyncSessionLocal() as db:
        try:
            await EquipmentInfoDAO(db).mark_idle_many(equipment_codes)
            await db.commit()
        except Exception:
            await db.rollback()
            raise

    task_inputs = [
        _demo_task(timestamp, "001", "A01", "A区取货点", "D01", "D区放货点", "URGENT", 100),
        _demo_task(timestamp, "002", "B01", "B区取货点", "E01", "E区放货点", "NORMAL", 50),
        _demo_task(timestamp, "003", "C01", "C区取货点", "F01", "F区放货点", "NORMAL", 10),
    ]

    created_tasks = []
    async with AsyncSessionLocal() as db:
        try:
            task_dao = AgvTaskDAO(db)
            log_dao = AgvTaskLogDAO(db)
            await task_dao.cancel_pending_demo_tasks()

            for item in task_inputs:
                # 模拟 WMS/MES 下发任务：创建 agv_task、outbox 事件和任务日志。
                task = AgvTaskORM(
                    task_code=item["task_code"],
                    start_storage_code=item["start_storage_code"],
                    start_storage_name=item["start_storage_name"],
                    end_storage_code=item["end_storage_code"],
                    end_storage_name=item["end_storage_name"],
                    task_type=1,
                    task_status=1,
                    task_detail_status=1,
                    priority_type=item["priority_type"],
                    priority_value=item["priority_value"],
                )
                db.add(task)
                await db.flush()
                db.add(
                    EventOutboxORM(
                        event_type="TASK_CREATED",
                        aggregate_type="TASK",
                        aggregate_id=str(task.task_id),
                        payload={
                            "task_id": task.task_id,
                            "task_code": task.task_code,
                            "start_storage_code": task.start_storage_code,
                            "end_storage_code": task.end_storage_code,
                        },
                        task_id=task.task_id,
                        state=1,
                    )
                )
                await log_dao.write_log(task.task_id, "SIM_CREATE", f"模拟任务创建: {task.task_code}")
                created_tasks.append(task.to_dict())

            await db.commit()
        except Exception:
            await db.rollback()
            raise

    return {"vehicles": vehicles, "tasks": created_tasks}


async def run_demo_flow(battery_threshold: float = 0.2) -> Dict[str, Any]:
    """一键跑通模拟闭环：先写模拟数据，再执行一轮调度。"""
    seeded = await seed_demo_data()
    scheduled = await run_schedule(battery_threshold=battery_threshold)
    return {"seeded": seeded, "schedule": scheduled}


def _demo_task(
    timestamp: str,
    suffix: str,
    start_code: str,
    start_name: str,
    end_code: str,
    end_name: str,
    priority_type: str,
    priority_value: int,
) -> dict:
    return {
        "task_code": f"SIM-{timestamp}-{suffix}",
        "start_storage_code": start_code,
        "start_storage_name": start_name,
        "end_storage_code": end_code,
        "end_storage_name": end_name,
        "priority_type": priority_type,
        "priority_value": priority_value,
    }
