from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from background.robot_monitor import run_robot_heartbeat_monitor
from common.exception.handler import register_exception_handlers
from core.conf import settings
from core.logger import configure_logging
from core.router import register_routers
from database.db import init_db
from database.seed import seed_database
from database.redis import close_redis


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """
    管理 FastAPI 应用启动和关闭期间的资源生命周期。
    """
    # FastAPI 现在推荐用 lifespan 管理启动/关闭逻辑，替代 on_event("startup")。
    await init_db()
    # async with SessionLocal() as session:
    #     await seed_database(session)
    stop_event = asyncio.Event()
    monitor_task = asyncio.create_task(
        run_robot_heartbeat_monitor(stop_event),
        name="robot-heartbeat-monitor",
    )
    try:
        yield
    finally:
        stop_event.set()
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        await close_redis()


def create_app() -> FastAPI:
    """
    创建并配置 FastAPI 应用，注册日志、异常处理器和业务路由。
    """
    # 先初始化日志，再创建应用并挂载异常处理器与业务路由。
    configure_logging()
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.debug,
        lifespan=lifespan,
    )
    register_exception_handlers(app)
    register_routers(app)
    return app
