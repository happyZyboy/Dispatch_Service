"""任务管理接口。"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.schemas import TaskSubmitRequest, TaskSubmitResponse
from app.services.task_service import get_pending_tasks, list_tasks, submit_task

router = APIRouter(tags=["任务管理"])


@router.post("/task/submit", response_model=TaskSubmitResponse)
async def submit_task_route(payload: TaskSubmitRequest):
    """接收 WMS/MES 下发的任务。"""
    try:
        task = await submit_task(payload)
        return TaskSubmitResponse(
            success=True,
            message="任务已创建, 等待调度",
            task_id=task["task_id"],
            task_code=task["task_code"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"任务创建失败: {str(e)}")


@router.get("/tasks/pending")
async def list_pending_tasks_route():
    """查询当前等待调度的任务。"""
    try:
        tasks = await get_pending_tasks()
        return {"total": len(tasks), "items": tasks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/tasks")
async def list_tasks_route(
    status: Optional[int] = Query(None, description="任务状态: 1-未开始 2-进行中 3-已完成"),
):
    """按状态查询任务列表；不传 status 时查询全部任务。"""
    try:
        tasks = await list_tasks(status)
        return {"total": len(tasks), "items": tasks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")
