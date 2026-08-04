from enum import IntEnum

class HolderStatus(IntEnum):
    """
    占位类型状态
    """
    #库位空闲
    IDLE = 0
    #库位禁用
    DISABLED = 1
    #库位货物占位
    FILLED = 2
    #库位预留占位
    PREPARING = 3
    #库位任务工作占位
    WORKING = 4


class Type(IntEnum):
    """库位类型枚举"""
    NORMAL = 0  # 普通库位
    INBOUND = 1  # 入库位
    OUTBOUND = 2  # 出库位
    TRANSFER = 3  # 中转库位
    CHARGING = 4  # 充电位
    WAITING = 5  # 等待位
    ABNORMAL = 6  # 异常暂存位