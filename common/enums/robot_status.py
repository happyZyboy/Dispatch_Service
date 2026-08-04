from enum import IntEnum


class RobotStatus(IntEnum):
    """
    机器人运行状态枚举。
    """

    # 机器人离线，调度器一般不会把它放进候选池。
    OFFLINE = 0
    # 机器人空闲可用，可以参与接单。
    IDLE = 1
    # 机器人正忙，通常已有任务在执行。
    BUSY = 2
    # 机器人正在充电，是否可接单取决于业务策略。
    CHARGE = 3
    # 机器人处于故障或异常状态，不能正常接单。
    FAULT = 4
    # 机器人被锁定或禁用，通常是人工干预后的状态。
    LOCKED = 5
