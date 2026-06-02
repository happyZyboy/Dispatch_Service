"""任务接口请求/响应模型。"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class TaskSubmitRequest(BaseModel):
    """WMS / MES 系统下发任务时使用的请求体。"""

    task_code: str = Field(..., description="任务编码, 全局唯一")
    start_storage_code: Optional[str] = Field(None, description="起点库位编码")
    start_storage_name: Optional[str] = Field(None, description="起点库位名称")
    end_storage_code: Optional[str] = Field(None, description="目标库位编码")
    end_storage_name: Optional[str] = Field(None, description="目标库位名称")
    task_type: int = Field(1, description="任务类型")
    priority_type: Optional[str] = Field(None, description="优先级类型")
    priority_value: int = Field(0, description="优先级数值")
    equipment_info_code: Optional[str] = Field(None, description="设备信息编码")
    equipment_info_name: Optional[str] = Field(None, description="设备信息名称")
    patrol_task_id: Optional[int] = Field(None, description="关联巡检任务ID")


class TaskSubmitResponse(BaseModel):
    """任务创建后的响应。"""

    success: bool = True
    message: str = "任务已创建, 等待调度"
    task_id: int
    task_code: str
