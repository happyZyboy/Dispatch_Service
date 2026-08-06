from enum import IntEnum

class HolderStatus(IntEnum):
    """
    地图节点运行时占位类型。
    """
    # 地图节点空闲
    IDLE = 0
    # 地图节点禁用
    DISABLED = 1
    # 地图节点货物占位
    FILLED = 2
    # 地图节点任务预占
    PREPARING = 3
    # 地图节点任务作业中
    WORKING = 4


class Type(IntEnum):
    """地图节点业务类型枚举。"""
    NORMAL = 0  # 普通导航节点
    INBOUND = 1  # 入库节点
    OUTBOUND = 2  # 出库节点
    TRANSFER = 3  # 中转节点
    CHARGING = 4  # 充电节点
    WAITING = 5  # 等待节点
    ABNORMAL = 6  # 异常暂存节点
