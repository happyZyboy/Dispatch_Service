from enum import IntEnum


class EnableStatus(IntEnum):
    """
    机器人启用状态
    """

    DISABLED = 0 #禁用
    ENABLED = 1 #启用

class Status(IntEnum):
    """
    机器人状态
    """

    OFFLINE = 0  # 离线
    IDLE = 1  # 空闲
    BUSY = 2  # 忙碌
    CHARGING = 3  # 充电
    FAULT = 4  # 故障
    LOCKED = 5  # 锁定