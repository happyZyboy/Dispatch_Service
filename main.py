"""
AMR 调度系统 FastAPI 入口。

启动:
    uvicorn main:app --host 127.0.0.1 --port 8000 --reload
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import amr, demo, monitor, schedule, system, tasks


def create_app() -> FastAPI:
    """创建 FastAPI 应用并挂载所有业务路由。"""
    app = FastAPI(
        title="AMR 调度系统 API",
        description="接收 AMR 心跳上报、WMS/MES 任务下发, 并执行调度匹配",
        version="1.0.0",
    )

    # 本地调试阶段允许跨域；生产环境建议改成明确的前端域名。
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # 接口层按业务模块拆分，main.py 只负责统一注册。
    app.include_router(system.router)
    app.include_router(amr.router)
    app.include_router(tasks.router)
    app.include_router(schedule.router)
    app.include_router(demo.router)
    app.include_router(monitor.router)
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
