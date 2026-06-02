"""任务相关 ORM 模型。"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict

from sqlalchemy import JSON, BigInteger, Column, DateTime, Integer, SmallInteger, String, Text, UniqueConstraint

from app.db.base import Base


class AgvTaskORM(Base):
    """AGV/AMR 任务主表。"""

    __tablename__ = "agv_task"

    task_id = Column(BigInteger, primary_key=True, autoincrement=True, comment="任务ID")
    task_code = Column(String(200), nullable=True, comment="任务编码")
    start_storage_id = Column(BigInteger, nullable=True, comment="起点库位ID")
    start_storage_code = Column(String(200), nullable=True, comment="起点库位编码")
    start_storage_name = Column(String(200), nullable=True, comment="起点库位名称")
    end_storage_id = Column(BigInteger, nullable=True, comment="目标库位ID")
    end_storage_code = Column(String(200), nullable=True, comment="目标库位编码")
    end_storage_name = Column(String(200), nullable=True, comment="目标库位名称")
    extra_storage_id = Column(BigInteger, nullable=True, comment="额外库位ID")
    task_status = Column(SmallInteger, default=1, comment="任务状态")
    task_detail_status = Column(Integer, default=1, comment="任务详细状态")
    task_type = Column(Integer, primary_key=True, default=1, comment="任务类型")
    agv_code = Column(String(200), nullable=True, comment="AGV车辆编码")
    priority_type = Column(String(50), nullable=True, comment="优先级类型")
    priority_value = Column(Integer, default=0, comment="优先级数值")
    create_time = Column(DateTime, default=datetime.now, comment="创建时间")
    modified_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="修改时间")
    equipment_info_id = Column(BigInteger, nullable=True)
    equipment_info_code = Column(String(255), nullable=True, comment="设备信息编码")
    equipment_info_name = Column(String(255), nullable=True, comment="设备信息名称")
    equipment_ip_addr = Column(String(255), nullable=True, comment="设备IP地址")
    equipment_type_code = Column(String(255), nullable=True, comment="设备类型编码")
    equipment_type_name = Column(String(255), nullable=True, comment="设备类型名称")
    patrol_task_id = Column(BigInteger, nullable=True, comment="巡检任务ID")

    __table_args__ = (UniqueConstraint("task_code", name="idx_agv_task_task_code"),)

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class AgvTaskLogORM(Base):
    """任务日志表。"""

    __tablename__ = "agv_task_log"

    task_log_id = Column(BigInteger, primary_key=True, autoincrement=True, comment="AGV任务日志ID")
    task_id = Column(BigInteger, nullable=True, comment="AGV任务ID")
    task_log_type = Column(String(200), nullable=True, comment="日志类型")
    log_content = Column(Text(length=16777215), nullable=True, comment="日志内容")
    create_time = Column(DateTime, default=datetime.now, comment="创建时间")
    modified_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="修改时间")

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class EventOutboxORM(Base):
    """事件发件箱表。

    当前项目里先用它记录“任务已创建/已调度”的事件状态。
    后续接 Redis、消息队列或成员A时，可以从这里扩展发布逻辑。
    """

    __tablename__ = "event_outbox"

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="自增主键")
    event_id = Column(String(50), nullable=False, default=lambda: f"evt_{uuid.uuid4().hex[:12]}", comment="事件ID")
    event_type = Column(String(50), nullable=False, comment="事件类型")
    aggregate_type = Column(String(20), default="TASK", comment="聚合类型")
    aggregate_id = Column(String(50), nullable=False, comment="关联业务ID")
    payload = Column(JSON, nullable=False, comment="事件数据载荷")
    version = Column(Integer, default=1, comment="事件结构版本")
    status = Column(String(20), default="PENDING", comment="发布状态")
    retry_count = Column(Integer, default=0, comment="重试次数")
    last_error = Column(Text, nullable=True, comment="最后错误信息")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    published_at = Column(DateTime, nullable=True, comment="发布时间")
    box_id = Column(BigInteger, nullable=True, comment="任务消费id")
    task_id = Column(BigInteger, nullable=True, comment="agv任务总表id")
    state = Column(Integer, nullable=True, comment="状态 1-待分配 2-已分配")
    create_time = Column(DateTime, default=datetime.now, comment="创建时间")
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="修改时间")

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
