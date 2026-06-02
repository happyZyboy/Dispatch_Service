"""数据库连接和 DAO 聚合入口。

通常会把连接配置、Session 工厂、事务上下文集中在这里。
业务代码通过 DatabaseManager 或具体 DAO 访问数据库，而不是到处创建连接。
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import quote_plus

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.db.dao.equipment import AmrVehicleStatusDAO, EquipmentInfoDAO, EquipmentLogDAO
from app.db.dao.patrol import PatrolTaskDAO, PatrolTaskDetDAO
from app.db.dao.storage import BaseStorageDAO
from app.db.dao.task import AgvTaskDAO, AgvTaskLogDAO, EventOutboxDAO

logger = logging.getLogger("db")

DB_CONFIG = {
    "user": "root",
    "password": "811995",
    "host": "127.0.0.1",
    "port": 3306,
    "database": "amr_db_1",
    "charset": "utf8mb4",
}

DATABASE_URL = (
    f"mysql+aiomysql://{DB_CONFIG['user']}:{quote_plus(DB_CONFIG['password'])}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    f"?charset={DB_CONFIG['charset']}"
)

engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,   # 取连接前先 ping，避免拿到 MySQL 已断开的旧连接。
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    echo=False,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


@asynccontextmanager
async def get_session() -> AsyncSession:
    """获取一个自动提交/回滚的异步 Session。"""
    async with AsyncSessionLocal() as db:
        try:
            yield db
            await db.commit()
        except Exception:
            await db.rollback()
            logger.exception("Database operation failed, rolled back.")
            raise


def get_db_manager() -> "DatabaseManager":
    """创建一个 DAO 聚合管理器。"""
    return DatabaseManager()


class DatabaseManager:
    """把多个 DAO 挂在同一个数据库事务里使用。

    用法:
        async with get_db_manager() as dbm:
            task = await dbm.agv_task.insert(...)
            await dbm.agv_task_log.write_log(...)

    退出 async with 时会自动 commit；发生异常时会 rollback。
    """

    def __init__(self):
        self._session: Optional[AsyncSession] = None

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()
        await self.close()
        return False

    async def open(self) -> AsyncSession:
        if self._session is None:
            self._session = AsyncSessionLocal()
        return self._session

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def commit(self) -> None:
        if self._session is not None:
            await self._session.commit()

    async def rollback(self) -> None:
        if self._session is not None:
            await self._session.rollback()

    @property
    def session(self) -> AsyncSession:
        """当前事务使用的 Session。"""
        if self._session is None:
            raise RuntimeError("Session is not opened. Use 'async with dbm:' first.")
        return self._session

    @property
    def agv_task(self) -> AgvTaskDAO:
        return AgvTaskDAO(self.session)

    @property
    def agv_task_log(self) -> AgvTaskLogDAO:
        return AgvTaskLogDAO(self.session)

    @property
    def event_outbox(self) -> EventOutboxDAO:
        return EventOutboxDAO(self.session)

    @property
    def equipment_info(self) -> EquipmentInfoDAO:
        return EquipmentInfoDAO(self.session)

    @property
    def amr_vehicle_status(self) -> AmrVehicleStatusDAO:
        return AmrVehicleStatusDAO(self.session)

    @property
    def equipment_log(self) -> EquipmentLogDAO:
        return EquipmentLogDAO(self.session)

    @property
    def base_storage(self) -> BaseStorageDAO:
        return BaseStorageDAO(self.session)

    @property
    def patrol_task(self) -> PatrolTaskDAO:
        return PatrolTaskDAO(self.session)

    @property
    def patrol_task_det(self) -> PatrolTaskDetDAO:
        return PatrolTaskDetDAO(self.session)

    async def execute_sql(self, sql: str, params: dict | None = None):
        result = await self.session.execute(text(sql), params or {})
        if sql.strip().upper().startswith("SELECT"):
            return result.fetchall()
        return result.rowcount
