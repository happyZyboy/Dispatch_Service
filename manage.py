from __future__ import annotations

import argparse
import asyncio

import uvicorn

from database.db import init_db


def main() -> None:
    """
    解析命令行参数，并启动 API 服务或执行数据库初始化命令。
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["run", "init-db", "scheduler", "rmf-consumer"], nargs="?", default="run")
    args = parser.parse_args()

    if args.command == "init-db":
        asyncio.run(init_db())
        return
    if args.command == "scheduler":
        from scheduler.worker import run_scheduler

        asyncio.run(run_scheduler())
        return
    if args.command == "rmf-consumer":
        from scheduler.rmf_consumer import run_rmf_consumer

        asyncio.run(run_rmf_consumer())
        return

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
