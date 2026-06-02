from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import BigInteger, Column, DateTime, Integer, SmallInteger, String, UniqueConstraint

from app.db.base import Base


class BaseStorageORM(Base):
    __tablename__ = "base_storage"

    storage_id = Column(BigInteger, primary_key=True, autoincrement=True, comment="储位ID")
    storage_code = Column(String(50), nullable=False, comment="储位编码")
    storage_name = Column(String(50), nullable=True, comment="储位名称")
    storage_status = Column(SmallInteger, default=0, comment="库位状态")
    if_enable = Column(SmallInteger, default=0, comment="是否启用")
    if_lock = Column(Integer, default=0, comment="是否锁住")
    passable_up = Column(SmallInteger, default=0, comment="上是否可通行")
    passable_down = Column(SmallInteger, default=0, comment="下是否可通行")
    passable_left = Column(SmallInteger, default=0, comment="左是否可通行")
    passable_right = Column(SmallInteger, default=0, comment="右是否可通行")
    status = Column(SmallInteger, default=1, comment="状态")
    create_time = Column(DateTime, default=datetime.now, comment="创建时间")
    modified_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="修改时间")
    work_status = Column(Integer, nullable=True, comment="工作状态")

    __table_args__ = (UniqueConstraint("storage_code", name="base_storage_code_idx"),)

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
