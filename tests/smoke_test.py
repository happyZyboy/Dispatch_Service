"""AMR 调度系统冒烟测试脚本。

运行方式:
    cd D:\调度算法\FastApi
    python tests\smoke_test.py

该脚本直接调用 service 层，不需要先启动 uvicorn。
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.amr_service import get_available_amrs
from app.services.demo_service import seed_demo_data
from app.services.monitor_service import get_system_state
from app.services.scheduler_service import run_schedule
from app.db.session import engine
from main import app


def assert_true(condition: bool, message: str) -> None:
    """简单断言工具，失败时抛出清晰错误。"""
    if not condition:
        raise AssertionError(message)


def print_json(title: str, data: Any) -> None:
    """统一打印 JSON 结果，方便人工查看。"""
    print(f"\n=== {title} ===")
    print(json.dumps(data, ensure_ascii=False, indent=2, default=str))


async def main() -> None:
    """执行一次完整的服务层冒烟测试。"""
    try:
        route_paths = {route.path for route in app.routes}
        required_routes = {"/health", "/amr/heartbeat", "/amr/available", "/task/submit", "/schedule", "/demo/run", "/state"}
        missing_routes = required_routes - route_paths
        assert_true(not missing_routes, f"路由缺失: {sorted(missing_routes)}")
        print("路由检查通过")

        seeded = await seed_demo_data()
        print_json("模拟数据", {"vehicles": len(seeded["vehicles"]), "tasks": len(seeded["tasks"])})
        assert_true(len(seeded["vehicles"]) == 3, "应写入 3 台模拟 AMR")
        assert_true(len(seeded["tasks"]) == 3, "应写入 3 条模拟任务")

        available_before = await get_available_amrs()
        print_json("调度前可用 AMR", available_before)
        assert_true(len(available_before) == 2, "默认电量阈值下应有 2 台可用 AMR")

        scheduled = await run_schedule()
        print_json("调度结果", scheduled)
        assert_true(scheduled["total_pending"] == 3, "本轮应有 3 条待分配任务")
        assert_true(scheduled["assigned_count"] == 2, "默认场景应成功分配 2 条任务")
        assert_true(scheduled["skipped_count"] == 1, "默认场景应跳过 1 条任务")

        assigned_codes = {item["equipment_info_code"] for item in scheduled["assignments"]}
        assert_true(assigned_codes == {"AMR01", "AMR02"}, f"分配车辆不符合预期: {assigned_codes}")

        state = await get_system_state()
        print_json("系统状态", {
            "total_amrs": state["total_amrs"],
            "available_amrs": state["available_amrs"],
            "pending_tasks": state["pending_tasks"],
            "running_tasks": state["running_tasks"],
        })
        assert_true(state["running_tasks"] >= 2, "调度后至少应有 2 条进行中任务")

        print("\n冒烟测试通过")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
