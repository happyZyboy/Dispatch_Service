from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.block.schema import BlockCallbackRequest
from app.block.service import handle_block_complete, handle_block_failed, handle_block_progress, handle_block_result
from common.response.response_schema import success
from database.db import get_db

router = APIRouter(prefix="/api/v1/rmf", tags=["rmf"])


@router.post("/block-result")
async def block_result(payload: BlockCallbackRequest, db: AsyncSession = Depends(get_db)):
    """
    接收 RMF 的流程块下发结果回调，并更新对应流程块状态。
    """
    return success(await handle_block_result(db, payload))


@router.post("/block-progress")
async def block_progress(payload: BlockCallbackRequest, db: AsyncSession = Depends(get_db)):
    """
    接收 RMF 的流程块执行进度回调，并保存当前进度信息。
    """
    return success(await handle_block_progress(db, payload))


@router.post("/block-complete")
async def block_complete(payload: BlockCallbackRequest, db: AsyncSession = Depends(get_db)):
    """
    接收 RMF 的流程块完成回调，并根据所有动作块状态推进任务。
    """
    return success(await handle_block_complete(db, payload))


@router.post("/block-failed")
async def block_failed(payload: BlockCallbackRequest, db: AsyncSession = Depends(get_db)):
    """
    接收 RMF 的流程块失败回调，并统一收口任务、机器人和地图节点状态。
    """
    return success(await handle_block_failed(db, payload))
