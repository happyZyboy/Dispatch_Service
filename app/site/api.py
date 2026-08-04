from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query, Path
from sqlalchemy.ext.asyncio import AsyncSession

from app.site.schema import SiteCreateRequest, SiteLockRequest, SiteReleaseRequest
from app.site.service import create_site, delete_site, list_sites, lock_site, release_site
from common.response.response_schema import success
from database.db import get_db
from typing import Annotated

router = APIRouter(prefix="/api/v1/sites", tags=["sites"])


@router.post("/craete")
async def create(payload: SiteCreateRequest, db: AsyncSession = Depends(get_db)):
    """
    创建库位基础信息，并返回生成后的库位记录。
    """
    return success(await create_site(db, payload))


@router.get("")
async def sites(
    page_size: int = Query(default=20, ge=1, le=100, alias="pageSize"),
    area: str | None = None,
    site_type: int | None = Query(default=None, alias="type"),
    disabled: int | None = None,
    filled: int | None = None,
    preparing: int | None = None,
    working: int | None = None,
    last_added_on: datetime | None = Query(default=None, alias="lastAddedOn"),
    last_id: int | None = Query(default=None, alias="lastId"),
    db: AsyncSession = Depends(get_db),
):
    """
    按区域、库位类型、禁用状态、占用状态和预占状态查询站点列表，使用游标分页处理深分页。
    """

    # 库位列表用于选点、占位和调度可用性判断。
    return success(
        await list_sites(
            db=db,
            page_size=page_size,
            area=area,
            site_type=site_type,
            disabled=disabled,
            filled=filled,
            preparing=preparing,
            working = working,
            last_added_on=last_added_on,
            last_id=last_id,
        )
    )




@router.delete("/{id}/del")
async def delete_worksite(
    worksite_id: Annotated[int, Path(alias="id")],
    db: AsyncSession = Depends(get_db),
):
    """
    逻辑删除指定库位，保留数据库历史记录。
    """
    return success(await delete_site(db, worksite_id))


@router.post("/{id}/lock")
async def lock(worksite_id: Annotated[int, Path(alias="id")], payload: SiteLockRequest, db: AsyncSession = Depends(get_db)):
    """
    通过holder类型来用于禁用库位、库位已被占、库位被预留等情况锁库位
    """
    return success(await lock_site(db, worksite_id, payload.agvId, payload.holder, payload.remark))


@router.post("/{id}/release")
async def release(worksite_id: Annotated[int, Path(alias="id")], payload: SiteReleaseRequest, db: AsyncSession = Depends(get_db)):
    """
    释放指定库位的禁用库位、库位已被占、库位被预留等情况的库位
    """
    # 释放接口只恢复占用标记，不改变站点基础定义。
    return success(await release_site(db, worksite_id, payload.clearFilled, payload.cleanPreparing, payload.cleanWorking,payload.cleanDisabled))

