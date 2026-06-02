"""AMR 接口请求/响应模型。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class AmrHeartbeatRequest(BaseModel):
    """AMR 车辆定时上报自身状态时使用的请求体。"""

    equipment_info_code: str = Field(..., description="AMR车辆唯一标识")
    equipment_info_name: Optional[str] = Field(None, description="AMR车辆名称")
    current_ip: Optional[str] = Field(None, description="AMR当前IP地址")
    pose_x: Optional[float] = Field(None, description="当前X坐标")
    pose_y: Optional[float] = Field(None, description="当前Y坐标")
    angle: Optional[float] = Field(None, description="当前角度")
    current_station: Optional[str] = Field(None, description="当前最近站点编码")
    last_station: Optional[str] = Field(None, description="上一个经过的站点编码")
    battery_level: Optional[float] = Field(None, description="电池电量, 0.0~1.0")
    battery_temp: Optional[float] = Field(None, description="电池温度")
    charging: Optional[bool] = Field(False, description="是否正在充电")
    voltage: Optional[float] = Field(None, description="电池电压")
    current: Optional[float] = Field(None, description="电池电流")
    has_err: Optional[bool] = Field(False, description="是否有故障")
    err_level: Optional[str] = Field(None, description="故障等级")
    err_json: Optional[Dict[str, Any]] = Field(None, description="故障详情JSON")
    blocked: Optional[bool] = Field(False, description="是否被阻挡")
    block_reason: Optional[str] = Field(None, description="阻挡原因")
    odo: Optional[float] = Field(None, description="累计行驶里程")
    vel_x: Optional[float] = Field(None, description="X方向线速度")
    vel_y: Optional[float] = Field(None, description="Y方向线速度")
    vel_ang: Optional[float] = Field(None, description="角速度")
    controller_temp: Optional[float] = Field(None, description="控制器温度")
    controller_humi: Optional[float] = Field(None, description="控制器湿度")
    controller_voltage: Optional[float] = Field(None, description="控制器电压")


class AmrStatusResponse(BaseModel):
    """AMR 心跳写入后的响应。"""

    success: bool = True
    message: str = "状态已更新"
    equipment_info_code: str


class AvailableAmrInfo(BaseModel):
    """可调度 AMR 的摘要信息。"""

    equipment_info_code: str
    equipment_info_name: Optional[str] = None
    current_station: Optional[str] = None
    battery_level: Optional[float] = None
    blocked: bool = False
    has_err: bool = False
    charging: bool = False
    last_heartbeat_time: Optional[datetime] = None


class AvailableAmrResponse(BaseModel):
    """可调度 AMR 列表响应。"""

    total: int
    available: list[AvailableAmrInfo]
