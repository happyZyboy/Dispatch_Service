"""任务业务服务。

接口层只接收 HTTP 请求，真正的任务创建、日志、outbox 写入在这里完成。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.db.models import AgvTaskORM, EventOutboxORM
from app.db.session import get_db_manager
from app.schemas.task import TaskSubmitRequest


async def submit_task(payload: TaskSubmitRequest) -> Dict[str, Any]:
    """创建一条待调度任务。

    同一个事务内写入:
    - agv_task 主任务
    - agv_task_log 创建日志
    - event_outbox 任务创建事件
    """
    dbm = get_db_manager()
    async with dbm:
        new_task = AgvTaskORM(
            task_code=payload.task_code,
            start_storage_code=payload.start_storage_code,
            start_storage_name=payload.start_storage_name,
            end_storage_code=payload.end_storage_code,
            end_storage_name=payload.end_storage_name,
            task_type=payload.task_type,
            task_status=1,
            task_detail_status=1,
            priority_type=payload.priority_type,
            priority_value=payload.priority_value,
            equipment_info_code=payload.equipment_info_code,
            equipment_info_name=payload.equipment_info_name,
            patrol_task_id=payload.patrol_task_id,
        )
        await dbm.agv_task.insert(new_task)
        await dbm.agv_task_log.write_log(
            new_task.task_id,
            "TASK_SUBMIT",
            f"任务 {new_task.task_code} 已创建, 等待调度",
        )
        await dbm.event_outbox.insert(
            EventOutboxORM(
                event_type="TASK_CREATED",
                aggregate_type="TASK",
                aggregate_id=str(new_task.task_id),
                payload={
                    "task_id": new_task.task_id,
                    "task_code": new_task.task_code,
                    "start_storage_code": new_task.start_storage_code,
                    "end_storage_code": new_task.end_storage_code,
                    "priority_value": new_task.priority_value,
                },
                task_id=new_task.task_id,
                state=1,
            )
        )
        return new_task.to_dict()


async def get_pending_tasks() -> List[Dict[str, Any]]:
    """查询当前所有待分配任务。"""
    dbm = get_db_manager()
    async with dbm:
        tasks = await dbm.agv_task.list_pending()
        return [task.to_dict() for task in tasks]


async def list_tasks(status: Optional[int] = None) -> List[Dict[str, Any]]:
    """按状态查询任务；status 为空时查询全部任务。"""
    dbm = get_db_manager()
    async with dbm:
        rows = await dbm.agv_task.list_by_status(status) if status is not None else await dbm.agv_task.list_all()
        return [row.to_dict() for row in rows]
