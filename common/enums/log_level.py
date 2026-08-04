from enum import StrEnum


class LogLevel(StrEnum):
    """
    任务日志级别枚举。
    """

    # 普通业务日志，用于记录正常流程事件。
    INFO = "INFO"
    # 警告日志，表示流程还能继续，但已有风险或异常信号。
    WARN = "WARN"
    # 错误日志，表示执行链路中出现失败或严重问题。
    ERROR = "ERROR"
