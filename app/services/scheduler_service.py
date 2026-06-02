"""任务调度服务。

当前版本使用一个简单贪心策略：
1. 高优先级任务先调度。
2. 优先选择当前站点等于任务起点的 AMR。
3. 没有站点匹配时，选择电量最高的 AMR。
"""

from __future__ import annotations

from typing import Any, Dict

from app.db.dao.equipment import EquipmentInfoDAO
from app.db.dao.task import AgvTaskDAO, AgvTaskLogDAO, EventOutboxDAO
from app.db.session import AsyncSessionLocal
from app.services.amr_service import BATTERY_THRESHOLD, get_available_amrs
from app.services.task_service import get_pending_tasks


async def run_schedule(battery_threshold: float = BATTERY_THRESHOLD) -> Dict[str, Any]:
    """执行一轮调度，把待分配任务绑定到可用 AMR。"""
    pending_tasks = await get_pending_tasks()
    if not pending_tasks:
        return {
            "success": True,
            "message": "没有待分配的任务",
            "total_pending": 0,
            "assigned_count": 0,
            "skipped_count": 0,
            "assignments": [],
        }

    available_amrs = await get_available_amrs(battery_threshold=battery_threshold)
    if not available_amrs:
        return {
            "success": True,
            "message": "没有可用的 AMR 车辆",
            "total_pending": len(pending_tasks),
            "assigned_count": 0,
            "skipped_count": len(pending_tasks),
            "assignments": [],
        }

    assignments = []
    # 本轮调度中，一台 AMR 最多分配一个任务，避免重复占用。
    assigned_codes: set[str] = set()

    async with AsyncSessionLocal() as db:
        try:
            task_dao = AgvTaskDAO(db)
            equipment_dao = EquipmentInfoDAO(db)
            log_dao = AgvTaskLogDAO(db)
            outbox_dao = EventOutboxDAO(db)

            for task in pending_tasks:
                # 排除本轮已经被分配出去的小车。
                candidates = [
                    amr for amr in available_amrs
                    if amr["equipment_info_code"] not in assigned_codes
                ]
                if not candidates:
                    break

                chosen_amr, match_reason = _choose_amr_for_task(task, candidates)
                task_id = task["task_id"]
                equipment_info_code = chosen_amr["equipment_info_code"]

                # 同一个事务内完成任务绑定、车辆置忙、事件发布、日志记录。
                await task_dao.assign_to_equipment(task_id, chosen_amr)
                await equipment_dao.update_state(equipment_info_code, state=1, task_id=task_id)
                await outbox_dao.mark_task_published(task_id)
                await log_dao.write_log(
                    task_id,
                    "SCHEDULE_ASSIGN",
                    f"任务 {task.get('task_code')} 分配给 {equipment_info_code}; {match_reason}",
                )

                assigned_codes.add(equipment_info_code)
                assignments.append(
                    {
                        "task_id": task_id,
                        "task_code": task.get("task_code"),
                        "equipment_info_code": equipment_info_code,
                        "equipment_info_id": chosen_amr.get("equipment_info_id"),
                        "start_storage_code": task.get("start_storage_code"),
                        "amr_current_station": chosen_amr.get("current_station"),
                        "battery_level": chosen_amr.get("battery_level"),
                        "match_reason": match_reason,
                    }
                )

            await db.commit()
        except Exception:
            await db.rollback()
            raise

    return {
        "success": True,
        "message": f"调度完成: 分配 {len(assignments)} 个任务, 跳过 {len(pending_tasks) - len(assignments)} 个",
        "total_pending": len(pending_tasks),
        "assigned_count": len(assignments),
        "skipped_count": len(pending_tasks) - len(assignments),
        "assignments": assignments,
    }


def _choose_amr_for_task(task: dict, candidates: list[dict]) -> tuple[dict, str]:
    """从候选 AMR 中选择最适合执行该任务的小车。"""
    start_station = task.get("start_storage_code")
    if start_station:
        for amr in candidates:
            if amr.get("current_station") == start_station:
                return (
                    amr,
                    f"站点精确匹配: AMR站点={amr.get('current_station')} 任务起点={start_station}",
                )

    chosen = max(candidates, key=lambda amr: amr.get("battery_level") or 0)
    return chosen, f"电量最优: battery={chosen.get('battery_level')}"
