from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.map.service import get_active_map_version
from app.site.enums import HolderStatus
from common.exception.base import MapNodeNotFoundError, ResourceUnavailableError
from database.models import MapNode


def _serialize_node(node: MapNode) -> dict:
    return {
        "id": node.id,
        "mapVersionId": str(node.map_version_id),
        "nodeCode": node.node_code,
        "nodeType": node.node_type,
        "x": float(node.x) if node.x is not None else None,
        "y": float(node.y) if node.y is not None else None,
        "ignoreDir": node.ignore_dir,
        "isEnabled": node.is_enabled,
        "agvId": node.agv_id,
        "filled": node.filled,
        "holder": node.holder,
        "preparing": node.preparing,
        "working": node.working,
        "addedOn": node.added_on.strftime("%Y-%m-%d %H:%M:%S") if node.added_on else None,
        "updatedOn": node.update_on.strftime("%Y-%m-%d %H:%M:%S") if node.update_on else None,
    }


async def list_sites(
    db: AsyncSession,
    page_size: int,
    node_type: str | None,
    enabled: int | None,
    filled: int | None,
    preparing: int | None,
    working: int | None,
    last_added_on: datetime | None = None,
    last_id: int | None = None,
) -> dict:
    """查询当前激活地图中的节点及其运行时占用状态。"""
    version = await get_active_map_version(db)
    conditions = [MapNode.map_version_id == version.id, MapNode.del_ == 0]
    if node_type:
        conditions.append(MapNode.node_type == node_type)
    if enabled is not None:
        conditions.append(MapNode.is_enabled == enabled)
    if filled is not None:
        conditions.append(MapNode.filled == filled)
    if preparing is not None:
        conditions.append(MapNode.preparing == preparing)
    if working is not None:
        conditions.append(MapNode.working == working)

    if last_added_on is not None and last_id is not None:
        conditions.append(
            or_(
                MapNode.added_on > last_added_on,
                and_(MapNode.added_on == last_added_on, MapNode.id > last_id),
            )
        )

    rows = (
        await db.scalars(
            select(MapNode)
            .where(*conditions)
            .order_by(MapNode.added_on.asc(), MapNode.id.asc())
            .limit(page_size + 1)
        )
    ).all()
    has_next = len(rows) > page_size
    rows = rows[:page_size]
    next_cursor = None
    if rows:
        last_row = rows[-1]
        next_cursor = {
            "lastAddedOn": last_row.added_on.strftime("%Y-%m-%d %H:%M:%S") if last_row.added_on else None,
            "lastId": last_row.id,
        }

    total = await db.scalar(
        select(func.count()).select_from(MapNode).where(*conditions)
    ) or 0
    return {
        "mapVersionId": str(version.id),
        "pageSize": page_size,
        "total": total,
        "hasNext": has_next,
        "nextCursor": next_cursor,
        "items": [_serialize_node(node) for node in rows],
    }


async def lock_site(
    db: AsyncSession,
    node_id: int,
    agv_id: str | None,
    holder: int,
    remark: str | None,
) -> dict:
    """更新地图节点的禁用、占用和预占状态。"""
    node = await _get_node(db, node_id)

    if not node.is_enabled and holder != HolderStatus.DISABLED:
        raise ResourceUnavailableError("地图节点已禁用")

    if holder == HolderStatus.DISABLED:
        node.is_enabled = 0
        node.filled = 0
        node.preparing = 0
        node.working = 0
    elif holder == HolderStatus.FILLED:
        node.filled = 1
        node.preparing = 0
        node.working = 0
    elif holder == HolderStatus.PREPARING:
        node.preparing = 1
        node.filled = 0
        node.working = 0
    elif holder == HolderStatus.WORKING:
        node.working = 1
        node.filled = 0
        node.preparing = 0

    node.holder = holder
    node.agv_id = agv_id
    await db.commit()
    await db.refresh(node)
    return _serialize_node(node)


async def release_site(
    db: AsyncSession,
    node_id: int,
    clear_filled: bool,
    clear_preparing: bool,
    clear_working: bool,
    clear_enabled: bool,
) -> dict:
    """释放地图节点的运行时占用状态。"""
    node = await _get_node(db, node_id)
    if not node.is_enabled and not clear_enabled:
        raise ResourceUnavailableError("地图节点已禁用")

    if clear_enabled:
        node.is_enabled = 1
    if clear_filled:
        node.filled = 0
    if clear_preparing:
        node.preparing = 0
    if clear_working:
        node.working = 0

    if not node.filled and not node.preparing and not node.working:
        node.holder = HolderStatus.IDLE
        node.agv_id = None

    await db.commit()
    await db.refresh(node)
    return _serialize_node(node)


async def _get_node(db: AsyncSession, node_id: int) -> MapNode:
    node = await db.scalar(
        select(MapNode).where(MapNode.id == node_id, MapNode.del_ == 0)
    )
    if not node:
        raise MapNodeNotFoundError()
    return node
