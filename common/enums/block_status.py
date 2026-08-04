from enum import IntEnum


class BlockStatus(IntEnum):
    """
    流程块状态枚举。
    """

    # 流程块已创建，但还没开始执行。
    CREATED = 10
    # 流程块已经开始执行，通常表示下游系统已接管。
    RUNNING = 20
    # 流程块执行成功，可以推进到下一块。
    SUCCESS = 30
    # 流程块执行失败，通常会导致整单失败或挂起。
    FAILED = 40
    # 流程块被取消，不再继续执行。
    CANCELLED = 50
