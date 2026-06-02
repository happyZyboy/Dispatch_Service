"""调度接口响应模型。"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class ScheduleResultItem(BaseModel):
    """单条任务分配结果。"""

    task_id: int
    task_code: str
    equipment_info_code: str
    equipment_info_id: Optional[int] = None
    start_storage_code: Optional[str] = None
    amr_current_station: Optional[str] = None
    battery_level: Optional[float] = None
    match_reason: str = ""


class ScheduleResponse(BaseModel):
    """一轮调度的汇总结果。"""

    success: bool = True
    message: str = "调度完成"
    total_pending: int = 0
    assigned_count: int = 0
    skipped_count: int = 0
    assignments: list[ScheduleResultItem] = []
