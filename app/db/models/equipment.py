"""设备与 AMR 状态 ORM 模型。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import JSON, BigInteger, Column, DateTime, Float, Integer, SmallInteger, String, Text, UniqueConstraint

from app.db.base import Base


class EquipmentInfoORM(Base):
    """设备基础信息表。

    一台 AMR 对应一条设备档案，保存设备编码、名称、IP、在线状态和调度状态。
    """

    __tablename__ = "wms_equipment_info"

    equipment_info_id = Column(BigInteger, primary_key=True, autoincrement=True, comment="设备ID")
    equipment_info_code = Column(String(100), nullable=True, comment="设备编码")
    equipment_info_name = Column(String(100), nullable=True, comment="设备名称")
    equipment_ip_addr = Column(String(100), nullable=True, comment="设备IP")
    equipment_type_id = Column(BigInteger, nullable=True, comment="设备类型ID")
    create_time = Column(DateTime, default=datetime.now, comment="创建时间")
    modified_user_name = Column(String(50), nullable=True, comment="修改人名称")
    modified_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="修改时间")
    if_auto_connect_tcp = Column(Integer, nullable=True, comment="是否自动连接TCP")
    if_online = Column(Integer, nullable=True, comment="是否在线")
    port = Column(String(100), nullable=True, comment="设备IP端口")
    state = Column(Integer, nullable=True, comment="状态 0-空闲 1-繁忙 2-故障")
    work_state = Column(String(200), nullable=True, comment="工作状态")
    update_time = Column(DateTime, nullable=True, comment="更新时间")
    task_id = Column(BigInteger, nullable=True, comment="agv任务总表id")

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class AmrVehicleStatusORM(Base):
    """AMR 实时状态表。

    保存 AMR 心跳上报的实时状态：位置、电量、速度、阻挡、故障等。
    equipment_info_code 与 wms_equipment_info.equipment_info_code 保持一致。
    """

    __tablename__ = "amr_vehicle_status"

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="自增主键")
    equipment_info_code = Column(String(100), nullable=False, comment="AMR车辆业务编码")
    equipment_info_name = Column(String(100), nullable=True, comment="AMR车辆名称")
    version = Column(String(100), nullable=True, comment="Robokit版本号")
    current_ip = Column(String(100), nullable=True, comment="当前IP地址")
    odo = Column(Float, nullable=True, comment="累计行驶里程(m)")
    today_odo = Column(Float, nullable=True, comment="今日行驶里程(m)")
    run_time_ms = Column(Float, nullable=True, comment="本次运行时间(ms)")
    total_time_ms = Column(Float, nullable=True, comment="累计运行时间(ms)")
    controller_temp = Column(Float, nullable=True, comment="控制器温度")
    controller_humi = Column(Float, nullable=True, comment="控制器湿度")
    controller_voltage = Column(Float, nullable=True, comment="控制器电压")
    pose_x = Column(Float, nullable=True, comment="X坐标")
    pose_y = Column(Float, nullable=True, comment="Y坐标")
    angle = Column(Float, nullable=True, comment="角度")
    confidence = Column(Float, nullable=True, comment="定位可信度")
    current_station = Column(String(100), nullable=True, comment="当前最近站点")
    last_station = Column(String(100), nullable=True, comment="上一个站点")
    vel_x = Column(Float, nullable=True, comment="X方向线速度")
    vel_y = Column(Float, nullable=True, comment="Y方向线速度")
    vel_ang = Column(Float, nullable=True, comment="角速度")
    blocked = Column(SmallInteger, default=0, comment="是否被阻挡")
    block_reason = Column(String(255), nullable=True, comment="阻挡原因")
    slowed = Column(SmallInteger, default=0, comment="是否减速")
    slow_reason = Column(String(255), nullable=True, comment="减速原因")
    battery_temp = Column(Float, nullable=True, comment="电池温度")
    battery_level = Column(Float, nullable=True, comment="电池电量")
    charging = Column(SmallInteger, default=0, comment="是否充电")
    voltage = Column(Float, nullable=True, comment="电压")
    current = Column(Float, nullable=True, comment="电流")
    max_charge_voltage = Column(Float, nullable=True, comment="最大充电电压")
    max_charge_current = Column(Float, nullable=True, comment="最大充电电流")
    manual_charge = Column(SmallInteger, default=0, comment="手动充电")
    auto_charge = Column(SmallInteger, default=0, comment="自动充电")
    battery_cycle = Column(Integer, nullable=True, comment="电池循环次数")
    has_err = Column(SmallInteger, default=0, comment="是否报错")
    err_level = Column(String(50), nullable=True, comment="报错等级")
    err_json = Column(JSON, nullable=True, comment="报错详情JSON")
    last_heartbeat_time = Column(DateTime, default=datetime.now, comment="最后心跳时间")
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    __table_args__ = (UniqueConstraint("equipment_info_code", name="uk_equipment_info_code"),)

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class EquipmentLogORM(Base):
    """设备日志表。"""

    __tablename__ = "wms_equipment_log"

    equipment_log_id = Column(BigInteger, primary_key=True, autoincrement=True, comment="设备日志ID")
    equipment_info_id = Column(String(100), nullable=True, comment="设备ID")
    equipment_info_code = Column(String(100), nullable=True, comment="设备编码")
    ip_addr = Column(String(100), nullable=True, comment="设备IP")
    log_content = Column(Text(length=16777215), nullable=True, comment="日志内容")
    log_level = Column(SmallInteger, nullable=True, comment="日志等级")
    create_user_id = Column(BigInteger, nullable=True, comment="创建人ID")
    create_user_name = Column(String(50), nullable=True, comment="创建人名称")
    create_time = Column(DateTime, default=datetime.now, comment="创建时间")
    modified_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="修改时间")
    update_time = Column(DateTime, nullable=True)

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
