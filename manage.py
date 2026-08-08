from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
import time
from pathlib import Path

import uvicorn

from database.db import init_db


_ALL_SERVICE_COMMANDS = ("run", "scheduler", "rmf-consumer")


def _start_service(command: str, *, reload: bool = True) -> subprocess.Popen:
    """
    启动一个独立的项目服务进程。

    :param command: 要执行的 manage 子命令。
    :param reload: 是否开启 API 自动重载，仅对 run 命令有效。
    :return: 已启动的子进程对象。
    """
    manage_path = Path(__file__).resolve()
    command_args = [sys.executable, str(manage_path), command]
    if command == "run" and not reload:
        command_args.append("--no-reload")
    return subprocess.Popen(command_args)


def _stop_services(processes: list[tuple[str, subprocess.Popen]]) -> None:
    """
    停止由 all 命令启动的全部服务进程。

    :param processes: 服务名称和对应子进程组成的列表。
    """
    for _, process in processes:
        if process.poll() is None:
            process.terminate()

    for _, process in processes:
        if process.poll() is None:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()


def run_all() -> None:
    """
    同时启动 API、调度 Worker 和 RMF 消费者。

    任意一个核心服务异常退出时，停止其余服务并结束 all 命令；
    用户按 Ctrl+C 时，也会统一停止全部子进程。
    """
    processes: list[tuple[str, subprocess.Popen]] = []
    try:
        for command in _ALL_SERVICE_COMMANDS:
            process = _start_service(command, reload=command != "run")
            processes.append((command, process))
            print(f"已启动 {command}，PID={process.pid}", flush=True)

        while True:
            for command, process in processes:
                return_code = process.poll()
                if return_code is not None:
                    raise RuntimeError(f"{command} 进程已退出，退出码={return_code}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("正在停止全部服务...", flush=True)
    except RuntimeError as exc:
        print(f"全部服务停止：{exc}", file=sys.stderr, flush=True)
        raise SystemExit(1) from exc
    finally:
        _stop_services(processes)


def main() -> None:
    """
    解析命令行参数，并启动 API、调度 Worker 或 RMF 消费者。
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=["run", "all", "init-db", "scheduler", "rmf-consumer"],
        nargs="?",
        default="run",
    )
    reload_group = parser.add_mutually_exclusive_group()
    reload_group.add_argument("--reload", dest="reload", action="store_true")
    reload_group.add_argument("--no-reload", dest="reload", action="store_false")
    parser.set_defaults(reload=True)
    args = parser.parse_args()

    if args.command == "all":
        run_all()
        return
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

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=args.reload)


if __name__ == "__main__":
    main()
