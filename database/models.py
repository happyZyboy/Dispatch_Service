from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import BigInteger, Boolean, DateTime, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from common.global_vars import id_worker
from common.utils import format_dt, now
from database.base import Base


class SerializableMixin:
    def to_dict(self) -> dict[str, Any]:
        """
        将 ORM 实体转换成接口字典，并统一格式化时间、Decimal 和布尔值。
        """
        # ORM 实体统一转成接口字典，顺手把时间和 Decimal 做格式化。
        result: dict[str, Any] = {}
        for column in self.__table__.columns:  # type: ignore[attr-defined]
            value = getattr(self, column.name)
            if isinstance(value, datetime):
                result[column.name] = format_dt(value)
            elif isinstance(value, Decimal):
                result[column.name] = float(value)
            elif isinstance(value, bool):
                result[column.name] = int(value)
            else:
                result[column.name] = value
        return result


class RobotItem(SerializableMixin, Base):
    __tablename__ = "t_robotitem"

    # 机器人档案表保留基础身份信息和启用状态。
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, default=id_worker.next_id)
    added_on: Mapped[datetime] = mapped_column(DateTime, default=now, nullable=False)
    del_: Mapped[int] = mapped_column("del", Integer, default=0, nullable=False)
    update_on: Mapped[datetime] = mapped_column(DateTime, default=now, onupdate=now, nullable=False)
    uuid: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    robot_code: Mapped[str | None] = mapped_column(String(64), default=None)
    robot_name: Mapped[str | None] = mapped_column(String(255), default=None)
    robot_type: Mapped[str | None] = mapped_column(String(64), default=None)
    enable_status: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    battery_threshold: Mapped[float | None] = mapped_column(Numeric(10, 2), default=50)
    current_map: Mapped[str | None] = mapped_column(String(255), default=None)


class RobotStatusRecord(SerializableMixin, Base):
    __tablename__ = "t_robotstatusrecord"

    # 状态流水负责保留历史，不做覆盖更新。
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, default=id_worker.next_id)
    duration: Mapped[int | None] = mapped_column(BigInteger, default=0)
    ended_on: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    location: Mapped[str | None] = mapped_column(String(255), default=None)
    new_status: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    odo: Mapped[float | None] = mapped_column(Numeric(19, 2), default=0)
    old_status: Mapped[int | None] = mapped_column(Integer, default=None)
    started_on: Mapped[datetime] = mapped_column(DateTime, default=now, nullable=False)
    today_odo: Mapped[float | None] = mapped_column(Numeric(19, 2), default=0)
    uuid: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    vehicle_name: Mapped[str | None] = mapped_column(String(255), default=None)


class RobotCurrentState(SerializableMixin, Base):
    __tablename__ = "robot_current_state"

    # 当前态快照只保留一行，给调度器做实时判断。
    robot_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    uuid: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    vehicle_name: Mapped[str | None] = mapped_column(String(255), default=None)
    current_status: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    dispatch_status: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    current_task_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    current_site_id: Mapped[str | None] = mapped_column(String(64), default=None)
    current_location: Mapped[str | None] = mapped_column(String(255), default=None)
    battery_level: Mapped[float | None] = mapped_column(Numeric(10, 2), default=100)
    has_unresolved_alarm: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    alarm_level: Mapped[str | None] = mapped_column(String(64), default=None)
    last_status_record_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    version: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=now, onupdate=now, nullable=False)


class WorkSite(SerializableMixin, Base):
    __tablename__ = "t_worksite"

    # 库位表同时承担站点、库位、等待位等位置资源的统一表达。
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, default=id_worker.next_id)
    agv_id: Mapped[str | None] = mapped_column(String(64), default=None)
    area: Mapped[str | None] = mapped_column(String(255), default=None)
    disabled: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    filled: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    group_name: Mapped[str | None] = mapped_column(String(255), default=None)
    holder: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    no: Mapped[str | None] = mapped_column(String(255), default=None)
    preparing: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    row_num: Mapped[int | None] = mapped_column(Integer, default=None)
    column_num: Mapped[int | None] = mapped_column(Integer, default=None)
    site_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    site_name: Mapped[str] = mapped_column(String(255), nullable=False)
    sync_failed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # `type` 是保留字段名，ORM 里用 `type_` 更安全。
    type_: Mapped[int] = mapped_column("type", Integer, default=0, nullable=False)
    working: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    remark: Mapped[str | None] = mapped_column(String(255), default=None)
    added_on: Mapped[datetime] = mapped_column(DateTime, default=now, nullable=False)
    update_on: Mapped[datetime] = mapped_column(DateTime, default=now, onupdate=now, nullable=False)
    del_: Mapped[int] = mapped_column("del", Integer, default=0, nullable=False)


class WindTaskDef(SerializableMixin, Base):
    __tablename__ = "t_windtaskdef"

    # 任务定义表存模板和编排描述，任务创建时会复制快照。
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, default=id_worker.next_id)
    create_date: Mapped[datetime] = mapped_column(DateTime, default=now, nullable=False)
    delay: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    detail: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    if_enable: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    label: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    period: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    periodic_task: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    project_id: Mapped[str | None] = mapped_column(String(255), default=None)
    release_sites: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    remark: Mapped[str | None] = mapped_column(String(255), default=None)
    status: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    template_name: Mapped[str | None] = mapped_column(String(255), default=None)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    windcategory_id: Mapped[int | None] = mapped_column(BigInteger, default=None)


class WindTaskRecord(SerializableMixin, Base):
    __tablename__ = "t_windtaskrecord"

    # 任务执行记录表是整条业务链路的主表。
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, default=id_worker.next_id)
    created_on: Mapped[datetime] = mapped_column(DateTime, default=now, nullable=False)
    def_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    def_label: Mapped[str | None] = mapped_column(String(150), default=None)
    def_version: Mapped[int | None] = mapped_column(Integer, default=None)
    ended_on: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    ended_reason: Mapped[str | None] = mapped_column(Text, default=None)
    input_params: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    status: Mapped[int] = mapped_column(Integer, default=20, nullable=False)
    task_def_detail: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    variables: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    agv_id: Mapped[str | None] = mapped_column(String(64), default=None)
    executor_time: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    first_executor_time: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    is_del: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    out_order_no: Mapped[str | None] = mapped_column(String(128), default=None)
    path: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    periodic_task: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    root_task_record_id: Mapped[int | None] = mapped_column(BigInteger, default=None)


class WindBlockRecord(SerializableMixin, Base):
    __tablename__ = "t_windblockrecord"

    # 流程块记录表负责把整单拆成一个个可回调的小步骤。
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, default=id_worker.next_id)
    block_config_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    block_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    block_input_params: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    block_input_params_value: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    block_internal_variables: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    block_name: Mapped[str] = mapped_column(String(255), nullable=False)
    ended_on: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    ended_reason: Mapped[str | None] = mapped_column(Text, default=None)
    input_params: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    internal_variables: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    order_id: Mapped[str | None] = mapped_column(String(128), default=None)
    output_params: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    started_on: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    status: Mapped[int] = mapped_column(Integer, default=10, nullable=False)
    task_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    task_record_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)


class WindTaskLog(SerializableMixin, Base):
    __tablename__ = "t_windtasklog"

    # 任务日志用于追踪每一次关键事件。
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, default=id_worker.next_id)
    create_time: Mapped[datetime] = mapped_column(DateTime, default=now, nullable=False)
    level: Mapped[str] = mapped_column(String(255), default="INFO", nullable=False)
    message: Mapped[str] = mapped_column(Text, default="", nullable=False)
    task_block_id: Mapped[int | None] = mapped_column(Integer, default=None)
    task_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    task_record_id: Mapped[int | None] = mapped_column(BigInteger, default=None)


class AlarmRecord(SerializableMixin, Base):
    __tablename__ = "t_alarmsrecord"

    # 报警表记录异常开始、结束和持续时间。
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, default=id_worker.next_id)
    alarms_code: Mapped[str] = mapped_column(String(255), nullable=False)
    alarms_cost_time: Mapped[float | None] = mapped_column(Numeric(19, 2), default=0)
    alarms_desc: Mapped[str | None] = mapped_column(String(1024), default=None)
    ended_on: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    level: Mapped[str] = mapped_column(String(255), default="WARNING", nullable=False)
    started_on: Mapped[datetime] = mapped_column(DateTime, default=now, nullable=False)
    # 同样避免直接占用 `type` 这个名字，减少歧义。
    type_: Mapped[int] = mapped_column("type", Integer, default=0, nullable=False)
    vehicle_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
