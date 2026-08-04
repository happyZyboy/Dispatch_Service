from enum import IntEnum


class DispatchStatus(IntEnum):
    """
    调度视角下的机器人状态枚举。
    """

    # 调度器看见机器人离线，不参与派单。
    OFFLINE = 0
    # 调度器看见机器人空闲，可作为候选车。
    IDLE = 1
    # 调度器看见机器人忙碌，暂不接新任务。
    BUSY = 2
    # 调度器看见机器人在充电，通常需要排除或降权。
    CHARGE = 3
    # 调度器看见机器人故障，必须从候选池中移除。
    FAULT = 4
    # 调度器看见机器人被锁定，禁止自动派单。
    LOCKED = 5
