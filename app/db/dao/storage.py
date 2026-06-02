from __future__ import annotations

from typing import Optional

from sqlalchemy import select

from app.db.dao.base import BaseDAO
from app.db.models.storage import BaseStorageORM


class BaseStorageDAO(BaseDAO):
    model = BaseStorageORM

    async def find_by_code(self, storage_code: str) -> Optional[BaseStorageORM]:
        result = await self.db.execute(
            select(self.model).where(self.model.storage_code == storage_code)
        )
        return result.scalar_one_or_none()
