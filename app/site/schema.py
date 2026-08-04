from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class SiteCreateRequest(BaseModel):
    """
    创建库位时使用的基础信息请求模型。
    """

    model_config = ConfigDict(populate_by_name=True)

    # siteId 是对外使用的业务编码，数据库内部 id 由雪花算法自动生成。
    siteId: str = Field(min_length=1, max_length=64)
    siteName: str = Field(min_length=1, max_length=255)
    area: str | None = Field(default=None, max_length=255)
    groupName: str | None = Field(default=None, max_length=255)
    rowNum: int | None = Field(default=None)
    columnNum: int | None = Field(default=None)
    site_type: int = Field(default=1, ge=1, alias="type")
    remark: str | None = Field(default=None, max_length=255)


class SiteLockRequest(BaseModel):
    # 锁定库位时可以顺手传一个占用方和备注。
    agvId: str | None = None
    holder: int = Field(default=0, ge=0, le=3)
    remark: str = Field(default="")


class SiteReleaseRequest(BaseModel):
    clearFilled: bool = False
    cleanPreparing: bool = False
    cleanWorking: bool = False
    cleanDisabled: bool = False
