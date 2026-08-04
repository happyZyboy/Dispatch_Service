from __future__ import annotations

from datetime import datetime

from sqlalchemy import Integer, and_, cast, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.site.enums import HolderStatus
from app.site.schema import SiteCreateRequest
from common.exception.base import (
    DuplicateRequestError,
    ResourceUnavailableError,
    SiteNotFoundError,
    StatusNotAllowedError,
)
from database.models import WorkSite


async def _next_site_no(db: AsyncSession) -> str:
    """
    查询当前最大的数字库位编号，并生成下一个三位数字编号。
    """
    max_no = await db.scalar(
        select(func.max(cast(WorkSite.no, Integer))).where(
            WorkSite.del_ == 0,
            WorkSite.no.is_not(None),
        )
    )
    return f"{int(max_no or 0) + 1:03d}"


async def create_site(db: AsyncSession, payload: SiteCreateRequest) -> dict:
    """
    创建一个新的库位，并初始化库位的运行状态。
    """
    existing = await db.scalar(
        select(WorkSite).where(WorkSite.site_id == payload.siteId)
    )
    if existing:
        raise DuplicateRequestError(f"库位已存在: {payload.siteId}")

    next_no = await _next_site_no(db)
    site = WorkSite(
        site_id=payload.siteId,
        site_name=payload.siteName,
        area=payload.area,
        group_name=payload.groupName,
        no=next_no,
        row_num=payload.rowNum,
        column_num=payload.columnNum,
        type_=payload.site_type,
        remark=payload.remark,
        disabled=0,
        filled=0,
        holder=HolderStatus.IDLE,
        preparing=0,
        sync_failed=0,
        working=0,
    )
    db.add(site)

    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise DuplicateRequestError(f"库位已存在: {payload.siteId}") from exc

    await db.refresh(site)
    result = site.to_dict()
    result["type"] = site.type_
    return result


async def delete_site(db: AsyncSession, worksite_id: int) -> dict:
    """
    逻辑删除指定库位，正在被任务使用的库位不允许删除。
    """
    site = await _get_site(db, worksite_id)

    # 库位仍在使用或被任务预留时，不能删除基础资源。
    if site.filled or site.preparing or site.working:
        raise StatusNotAllowedError("库位正在使用或已被任务预留，不能删除")

    # 只修改删除标记，保留历史数据，避免破坏任务和日志中的关联信息。
    site.del_ = 1
    await db.commit()
    await db.refresh(site)

    result = site.to_dict()
    result["type"] = site.type_
    result["deleted"] = True
    return result


async def list_sites(
    db: AsyncSession,
    page_size: int,
    area: str | None,
    site_type: int | None,
    disabled: int | None,
    filled: int | None,
    preparing: int | None,
    working: int | None,
    last_added_on: datetime | None = None,
    last_id: int | None = None
) -> dict:
    """
    查询站点数据，游标分页。
    """
    conditions:list = [WorkSite.del_ == 0]
    if area:
        conditions.append(WorkSite.area == area)
    if site_type is not None:
        conditions.append(WorkSite.type_ == site_type)
    if disabled is not None:
        conditions.append(WorkSite.disabled == disabled)
    if filled is not None:
        conditions.append(WorkSite.filled == filled)
    if preparing is not None:
        conditions.append(WorkSite.preparing == preparing)
    if working is not None:
        conditions.append(WorkSite.working == working)

    # 游标条件：从上一页最后一条记录继续往后查
    if last_added_on is not None and last_id is not None:
        conditions.append(
            or_(
                WorkSite.added_on > last_added_on,
                and_(
                    WorkSite.added_on == last_added_on,
                    WorkSite.id > last_id
                )
            )
        )

    stmt = (
        select(WorkSite)
        .where(*conditions)
        .order_by(WorkSite.added_on.asc(), WorkSite.id.asc())
        .limit(page_size + 1)
    )

    rows = (await db.scalars(stmt)).all()
    has_next = len(rows) > page_size
    rows = rows[:page_size]

    items = []
    for site in rows:
        payload = site.to_dict()
        payload["type"] = site.type_
        items.append(payload)

    next_cursor = None
    if rows:
        last_row = rows[-1]
        next_cursor = {
            "lastAddedOn": last_row.added_on.strftime("%Y-%m-%d %H:%M:%S") if last_row.added_on else None,
            "lastId": last_row.id,
        }

    total = await db.scalar(
        select(func.count()).select_from(WorkSite).where(WorkSite.del_ == 0)
    ) or 0

    return {
        "pageSize": page_size,
        "total": total,
        "hasNext": has_next,  #是否存在下一页
        "nextCursor": next_cursor,  #包含上一条创建时间和id的游标
        "items": items,
    }


async def lock_site(db: AsyncSession, worksite_id: int, agv_id: str | None, holder: int, remark: str | None) -> dict:
    """
    更新指定库位的禁用库位、库位已被占、库位被预留，库位正在工作等情况，并返回更新后的站点数据。
    """
    site = await _get_site(db, worksite_id)

    # 先检查是否已被禁用
    if site.disabled == 1:
        raise ResourceUnavailableError("库位已被禁用")

    # 根据不同操作更新状态
    if holder == HolderStatus.DISABLED:
        site.disabled = 1
        site.filled = 0
        site.preparing = 0
        site.working = 0
        site.remark = remark or "管理员禁用"

    elif holder == HolderStatus.FILLED:
        site.filled = 1
        site.preparing = 0
        site.working = 0
        site.remark = remark or f"库位正被{agv_id}占用中"

    elif holder == HolderStatus.PREPARING:
        site.preparing = 1
        site.filled = 0
        site.working = 0
        site.remark = remark or f"库位为{agv_id}预留中"

    elif holder == HolderStatus.WORKING:
        site.working = 1
        site.filled = 0
        site.preparing = 0

    # 记录操作的 AGV
    site.agv_id = agv_id

    await db.commit()

    # 返回结果
    payload = site.to_dict()
    payload["type"] = site.type_
    return payload


async def release_site(db: AsyncSession, worksite_id: int, clear_filled: bool, clear_preparing: bool, clear_working: bool, clear_disabled: bool) -> dict:
    """
    清除当前库位状态。
    """
    site = await _get_site(db, worksite_id)

    # 检查是否已被禁用
    if site.disabled == 1:
        raise ResourceUnavailableError("库位已被禁用")
    elif clear_disabled:
        site.disabled = 0

    if clear_filled:
        site.filled = 0

    if clear_preparing:
        site.preparing = 0

    if clear_working:
        site.working = 0

    if clear_disabled:
        site.disabled = 0

    # 所有占用状态都清除后，恢复库位的基础空闲状态。
    if not site.filled and not site.preparing and not site.working:
        site.holder = HolderStatus.IDLE
        site.agv_id = None
        site.remark = None

    await db.commit()
    payload = site.to_dict()
    payload["type"] = site.type_
    return payload



async def _get_site(db: AsyncSession, worksite_id: int) -> WorkSite:
    """
    根据站点编号查询站点，不存在时抛出站点不存在异常。
    """
    site = await db.scalar(select(WorkSite).where(WorkSite.id == worksite_id, WorkSite.del_ == 0))
    if not site:
        raise SiteNotFoundError()
    return site
