from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from core.conf import settings
from database.base import Base

if not settings.database_url.startswith("mysql+asyncmy://"):
    raise ValueError("DATABASE_URL 必须使用 mysql+asyncmy:// 连接格式")

engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    #连接探活
    pool_pre_ping=True,
    #存活超时回收重建
    pool_recycle=3600,
    pool_size=10,
    #额外连接
    max_overflow=10,
    pool_timeout=30
)

SessionLocal = async_sessionmaker(
    bind=engine,
    autoflush=False,
    expire_on_commit=False,
    class_=AsyncSession
)


async def get_db() -> AsyncIterator[AsyncSession]:
    """
    为每个请求创建一个异步数据库会话，并在请求结束后自动释放。
    """
    # 每个请求拿一个独立的异步 Session，请求结束后自动释放。
    async with SessionLocal() as db:
        yield db


async def init_db() -> None:
    """
    导入全部 ORM 模型并根据模型元数据创建缺失的数据表。
    """
    # 初版仍然直接根据 ORM 模型建表，后面再平滑切到 Alembic。
    from database import models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
