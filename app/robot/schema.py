from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class RobotHeartbeatRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    # 心跳上报尽量兼容外部系统已有字段命名。

    #表robot_current_state字段
    uuid: str
    vehicleName: str | None = None
    currentStatus: int
    dispatchStatus: int = Field(default=1, ge=0, le=5)
    currentTaskId: int | None = None
    currentSiteId: str | None = None
    currentLocation: str | None = None
    batteryLevel: float
    hasUnresolvedAlarm: int
    alarmLevel: str | None = None
    #表t_robotitem 字段
    robotType: str | None = None
    enableStatus: int
    currentMap: str | None = None
    #表t_robotstatusrecord字段
    odo: float
    todayOdo: float


class RobotBatteryThresholdRequest(BaseModel):
    """更新机器人参与自动调度的最低电量阈值。"""

    batteryThreshold: float = Field(..., ge=0, le=100)


class RobotHistoryStateListQuery(BaseModel):
    # 小车历史状态列表查询
    uuid: str
    page_size: int = Field(default=20, ge=1, le=100, alias="pageSize")
    last_started_on: datetime | None = Field(default=None, alias="lastStartedOn")
    last_id: int | None = Field(default=None, alias="lastId")

class RobotCurrentStateListQuery(BaseModel):
    # 当前态列表查询，主要给调度筛车和前端展示使用。
    page: int = Field(default=1, ge=1),
    page_size: int = Field(default=20, ge=1, le=100, alias="pageSize"),
    dispatch_status: int | None = Field(default=None, ge=0, le=5, alias="dispatchStatus"),
    uuid: str | None = None,
