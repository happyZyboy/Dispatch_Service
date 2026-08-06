from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.site.schema import SiteLockRequest, SiteReleaseRequest
from app.site.service import list_sites, lock_site, release_site
from common.response.response_schema import success
from database.db import get_db


router = APIRouter(prefix="/api/v1/map-nodes", tags=["地图节点"])


@router.get("")
async def nodes(
    page_size: int = Query(default=20, ge=1, le=100, alias="pageSize"),
    node_type: str | None = Query(default=None, alias="nodeType"),
    enabled: int | None = Query(default=None, ge=0, le=1),
    filled: int | None = Query(default=None, ge=0, le=1),
    preparing: int | None = Query(default=None, ge=0, le=1),
    working: int | None = Query(default=None, ge=0, le=1),
    last_added_on: datetime | None = Query(default=None, alias="lastAddedOn"),
    last_id: int | None = Query(default=None, alias="lastId"),
    db: AsyncSession = Depends(get_db),
):
    """查询当前激活地图的节点和运行时占用状态。"""
    return success(
        await list_sites(
            db=db,
            page_size=page_size,
            node_type=node_type,
            enabled=enabled,
            filled=filled,
            preparing=preparing,
            working=working,
            last_added_on=last_added_on,
            last_id=last_id,
        )
    )


@router.post("/{node_id}/lock")
async def lock(node_id: int, payload: SiteLockRequest, db: AsyncSession = Depends(get_db)):
    """更新地图节点的禁用、占用或预占状态。"""
    return success(await lock_site(db, node_id, payload.agvId, payload.holder, payload.remark))


@router.post("/{node_id}/release")
async def release(node_id: int, payload: SiteReleaseRequest, db: AsyncSession = Depends(get_db)):
    """释放地图节点的运行时占用状态。"""
    return success(
        await release_site(
            db,
            node_id,
            payload.clearFilled,
            payload.clearPreparing,
            payload.clearWorking,
            payload.clearEnabled,
        )
    )
