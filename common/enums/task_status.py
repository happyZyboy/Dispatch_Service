from enum import IntEnum


class TaskStatus(IntEnum):
    """
    任务主状态枚举。
    """

    # 任务记录刚创建，通常只在内部初始化阶段短暂出现。
    CREATED = 10
    # 任务已入库，等待调度器分配机器人。
    PENDING_ASSIGN = 20
    # 任务已经选到机器人，但还没真正下发执行动作。
    ASSIGNED = 30
    DISPATCHING = 35
    # 任务已经下发到执行系统或下游桥接服务。
    DISPATCHED = 40
    # 任务正在执行过程中，至少有一个动作块处于运行中。
    EXECUTING = 50
    # 任务所有步骤都已完成，整单正常结束。
    COMPLETED = 60
    # 任务被人工或系统取消，不再继续执行。
    CANCELLED = 70
    # 任务执行失败，通常需要人工排查或触发重试。
    FAILED = 80
    # 任务因异常被挂起，等待后续恢复或人工处理。
    SUSPENDED = 90
