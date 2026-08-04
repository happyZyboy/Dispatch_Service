from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class AlarmReportRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    # 报警上报通常来自机器人或外部监控系统。
    vehicleId: str
    alarmsCode: str
    alarmsDesc: str
    level: str = "WARNING"
    type: int = 0


class AlarmRecoverRequest(BaseModel):
    # 恢复报警时只需要一个可选说明。
    reason: str | None = None


class AlarmListQuery(BaseModel):
    # 报警列表也按统一分页结构来走。
    page: int = Field(default=1, ge=1)
    pageSize: int = Field(default=20, ge=1, le=100)
