from __future__ import annotations

from datetime import datetime
from typing import Dict

from fastapi import APIRouter

router = APIRouter(tags=["系统"])


@router.get("/health")
async def health() -> Dict[str, str]:
    return {
        "status": "ok",
        "service": "amr-dispatch-system",
        "time": datetime.now().isoformat(timespec="seconds"),
    }
