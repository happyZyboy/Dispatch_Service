from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BlockCallbackRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    taskRecordId: int
    blockId: str | None = None
    blockName: str | None = None
    orderId: str | None = None
    message: str | None = None
    progress: int | None = None
    detail: dict[str, Any] = Field(default_factory=dict)
    selectedAgvId: str | None = None
