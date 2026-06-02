"""模拟流程接口。"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.services.demo_service import run_demo_flow, seed_demo_data

router = APIRouter(prefix="/demo", tags=["模拟流程"])


@router.post("/seed")
async def demo_seed():
    """写入模拟 AMR 和待调度任务。"""
    try:
        result = await seed_demo_data()
        return {
            "success": True,
            "message": "模拟数据已写入",
            "vehicles": result["vehicles"],
            "tasks": result["tasks"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"模拟数据写入失败: {str(e)}")


@router.post("/run")
async def demo_run(
    battery_threshold: float = Query(0.2, description="电量最低阈值, 默认 20%"),
):
    """一键跑通模拟闭环。"""
    try:
        result = await run_demo_flow(battery_threshold=battery_threshold)
        return {
            "success": True,
            "message": "模拟闭环已跑完",
            "seeded": result["seeded"],
            "schedule": result["schedule"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"模拟流程执行失败: {str(e)}")
