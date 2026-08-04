from fastapi import FastAPI

from app.alarm.api import router as alarm_router
from app.block.api import router as block_router
from app.dispatch.api import router as dispatch_router
from app.robot.api import router as robot_router
from app.site.api import router as site_router
from app.system.api import router as system_router
from app.task.api import router as task_router


def register_routers(app: FastAPI) -> None:
    """
    将各业务模块的 APIRouter 统一注册到 FastAPI 应用对象中。
    """
    # 路由注册集中在这里，方便后面按业务域继续拆分或摘除。
    app.include_router(system_router)
    app.include_router(task_router)
    app.include_router(robot_router)
    app.include_router(site_router)
    app.include_router(dispatch_router)
    app.include_router(block_router)
    app.include_router(alarm_router)
