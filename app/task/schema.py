from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class TaskSubmitRequest(BaseModel):
    # WMS 只提交按执行顺序排列的目标库位，实际起点由调度阶段选车后补出。
    templateLabel: str | None = None  # 任务模板业务编码，不传时使用默认模板
    sitePath: list[str] = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("sitePath", "siteList", "path"),
    )  # WMS 按顺序提交的目标库位列表
    priority: int = Field(default=5, ge=1, le=10)  # 任务优先级，范围 1-10，默认 5
    agvId: str | None = None  # 指定执行任务的机器人编码，为空则由系统调度分配
    outOrderNo: str | None = None  # 外部系统订单号，用于业务侧关联追踪
    periodicTask: int = 0  # 是否周期任务，0 表示否，1 表示是
    remark: str | None = None  # 任务备注信息


class TaskCancelRequest(BaseModel):
    # 取消任务只保留一个原因字段，方便落日志。
    reason: str | None = None  # 取消原因，会写入任务结束原因和日志


class TaskRetryRequest(BaseModel):
    # 重试任务和取消类似，也只带一个简短原因。
    reason: str | None = None  # 重试原因，用于记录重试操作日志


class TaskListQuery(BaseModel):
    model_config = ConfigDict(extra="ignore")

    # 列表查询参数专门做分页和简单筛选。
    page: int = Field(default=1, ge=1)  # 当前页码，从 1 开始
    pageSize: int = Field(default=20, ge=1, le=100)  # 每页数量，范围 1-100，默认 20
    status: int | None = None  # 任务状态筛选
    agvId: str | None = None  # 按执行机器人编码筛选
    fromSite: str | None = None  # 按起点站点筛选
    toSite: str | None = None  # 按终点站点筛选
    keyword: str | None = None  # 关键字筛选，匹配任务 ID 或外部订单号


class TaskDispatchRequest(BaseModel):
    # 手工触发调度时，用这组参数指定任务和车辆。
    taskId: str | None = None  # 指定要触发调度的任务 ID，为空则自动选择待调度任务
    agvId: str | None = None  # 指定调度机器人编码，为空则由系统选择
    force: bool = False  # 是否强制调度，true 时跳过部分状态校验


class TaskRemarkPayload(BaseModel):
    # 预留的通用扩展载体，后续可以塞更多临时字段。
    data: dict[str, Any] = Field(default_factory=dict)  # 通用扩展数据载体
