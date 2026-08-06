from __future__ import annotations

from pydantic import BaseModel, Field


class SiteLockRequest(BaseModel):
    # 锁定地图节点时可以顺手传一个占用方和备注。
    agvId: str | None = None
    holder: int = Field(default=0, ge=0, le=4)
    remark: str = Field(default="")


class SiteReleaseRequest(BaseModel):
    clearFilled: bool = False
    clearPreparing: bool = False
    clearWorking: bool = False
    clearEnabled: bool = False
