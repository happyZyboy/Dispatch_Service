from __future__ import annotations

from typing import Any, List, Optional

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession


class BaseDAO:
    """通用 DAO 基类，提供所有表可复用的基础 CRUD 方法。"""

    model: type = None

    def __init__(self, session: AsyncSession):
        """绑定当前数据库会话。"""
        self.db = session

    async def insert(self, obj):
        """新增一条记录。"""
        self.db.add(obj)
        await self.db.flush()
        return obj

    async def bulk_insert(self, objects: List) -> None:
        """批量新增记录。"""
        self.db.add_all(objects)
        await self.db.flush()

    async def delete(self, obj) -> None:
        """删除指定 ORM 对象。"""
        await self.db.delete(obj)
        await self.db.flush()

    async def delete_by_id(self, pk: Any, pk_col: str | None = None) -> int:
        """按主键删除记录。"""
        pk_col = pk_col or self.model.__table__.primary_key.columns.keys()[0]
        result = await self.db.execute(delete(self.model).where(getattr(self.model, pk_col) == pk))
        await self.db.flush()
        return result.rowcount

    async def update_by_id(self, pk: Any, updates: dict, pk_col: str | None = None) -> int:
        """按主键更新记录。"""
        pk_col = pk_col or self.model.__table__.primary_key.columns.keys()[0]
        result = await self.db.execute(
            update(self.model).where(getattr(self.model, pk_col) == pk).values(**updates)
        )
        await self.db.flush()
        return result.rowcount

    async def get_by_id(self, pk: Any, pk_col: str | None = None) -> Optional:
        """按主键查询单条记录。"""
        pk_col = pk_col or self.model.__table__.primary_key.columns.keys()[0]
        result = await self.db.execute(select(self.model).where(getattr(self.model, pk_col) == pk))
        return result.scalar_one_or_none()

    async def list_all(self) -> List:
        """查询全部记录。"""
        result = await self.db.execute(select(self.model))
        return result.scalars().all()

    async def list_by_filter(self, **kwargs) -> List:
        """按字段等值条件查询记录列表。"""
        result = await self.db.execute(select(self.model).filter_by(**kwargs))
        return result.scalars().all()
