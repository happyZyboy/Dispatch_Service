from __future__ import annotations

from pydantic import BaseModel


class DispatchTriggerRequest(BaseModel):
    """
    手工触发任务调度的请求参数模型。

    :return: 无直接返回值；该类用于校验调度触发请求字段。
    """

    taskId: int | None = None  # 指定要调度的任务 ID，不传时自动选择待调度任务
    agvId: str | None = None  # 指定执行任务的机器人 ID，不传时自动选择机器人
    force: bool = False  # 强制跳过任务状态校验，重试调度
