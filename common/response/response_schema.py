from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from common.utils import now


def _timestamp_ms() -> int:
    """
    生成当前时间的毫秒级时间戳，写入统一响应的 timestamp 字段。
    """
    # 统一返回毫秒时间戳，方便前端直接展示和排序。
    return int(now().timestamp() * 1000)


class ResponseSchema(BaseModel):
    code: int = 0
    message: str = "success"
    data: Any = None
    timestamp: int = Field(default_factory=_timestamp_ms)  # Field 能附加上约束、描述、默认值等信息 用于pydantic做数据校验


def success(data: Any = None, message: str = "success") -> ResponseSchema:
    """
    创建统一格式的成功响应对象。
    """
    # 所有成功响应保持同一结构，减少接口风格分裂。
    return ResponseSchema(code=0, message=message, data=data)


def failure(code: int, message: str, data: Any = None) -> ResponseSchema:
    """
    创建统一格式的失败响应对象，并保存错误码、错误消息和附加数据。
    """
    return ResponseSchema(code=code, message=message, data=data)
