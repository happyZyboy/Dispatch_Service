from enum import StrEnum


class AlarmLevel(StrEnum):
    """
    报警级别枚举。
    """

    # 一般性告警，通常用于提示需要关注但不一定立即停机。
    WARNING = "WARNING"
    # 严重告警，通常会影响任务执行或机器人可用性。
    ERROR = "ERROR"
    # 致命告警，通常需要立即人工介入处理。
    FATAL = "FATAL"
