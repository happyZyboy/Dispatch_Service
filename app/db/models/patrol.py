from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import BigInteger, Column, DateTime, String

from app.db.base import Base


class PatrolTaskORM(Base):
    __tablename__ = "wms_patrol_task"

    patrol_task_id = Column(BigInteger, primary_key=True, autoincrement=True, comment="巡检任务ID")
    patrol_task_code = Column(String(100), nullable=True, comment="巡检任务编码")
    patrol_path_code = Column(String(100), nullable=True, comment="巡检路线编码")
    patrol_status = Column(BigInteger, nullable=True, comment="巡检任务状态")
    equipment_info_code = Column(String(100), nullable=True, comment="设备编码")
    create_time = Column(DateTime, default=datetime.now, comment="创建时间")
    modified_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="修改时间")

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class PatrolTaskDetORM(Base):
    __tablename__ = "wms_patrol_task_det"

    patrol_task_det_id = Column(BigInteger, primary_key=True, autoincrement=True, comment="巡检任务明细ID")
    patrol_task_id = Column(BigInteger, nullable=True, comment="巡检任务ID")
    storage_code = Column(String(100), nullable=True, comment="巡检点位编码")
    patrol_det_status = Column(BigInteger, nullable=True, comment="巡检明细状态")
    equipment_info_code = Column(String(100), nullable=True, comment="设备编码")
    create_time = Column(DateTime, default=datetime.now, comment="创建时间")
    modified_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="修改时间")

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
