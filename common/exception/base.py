from __future__ import annotations

from typing import Any


class AppError(Exception):
    """
    项目内所有业务异常的父类。
    """

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        """
        保存统一异常码、异常消息和附加数据，供全局异常处理器返回接口响应。
        """
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


class TaskNotFoundError(AppError):
    def __init__(self, message: str = "任务不存在", data: Any = None) -> None:
        """
        创建任务不存在异常，并固定使用错误码 10001。
        """
        super().__init__(10001, message, data)


class RobotNotFoundError(AppError):
    def __init__(self, message: str = "机器人不存在", data: Any = None) -> None:
        """
        创建机器人不存在异常，并固定使用错误码 10002。
        """
        super().__init__(10002, message, data)


class SiteNotFoundError(AppError):
    def __init__(self, message: str = "站点不存在", data: Any = None) -> None:
        """
        创建站点不存在异常，并固定使用错误码 10003。
        """
        super().__init__(10003, message, data)


class TemplateNotFoundError(AppError):
    def __init__(self, message: str = "模板不存在", data: Any = None) -> None:
        """
        创建任务模板不存在异常，并固定使用错误码 10004。
        """
        super().__init__(10004, message, data)


class AlarmNotFoundError(AppError):
    def __init__(self, message: str = "报警不存在", data: Any = None) -> None:
        """
        创建报警不存在异常，并固定使用错误码 10005。
        """
        super().__init__(10005, message, data)


class RequestParamError(AppError):
    def __init__(self, message: str = "参数校验失败", data: Any = None) -> None:
        """
        创建请求参数校验异常，并固定使用错误码 10011。
        """
        super().__init__(10011, message, data)


class StatusNotAllowedError(AppError):
    def __init__(self, message: str = "状态不允许当前操作", data: Any = None) -> None:
        """
        创建状态不允许当前操作异常，并固定使用错误码 10012。
        """
        super().__init__(10012, message, data)


class ResourceUnavailableError(AppError):
    def __init__(self, message: str = "资源不可用", data: Any = None) -> None:
        """
        创建资源不可用异常，并固定使用错误码 10013。
        """
        super().__init__(10013, message, data)


class DuplicateRequestError(AppError):
    def __init__(self, message: str = "重复请求", data: Any = None) -> None:
        """
        创建重复请求异常，并固定使用错误码 10014。
        """
        super().__init__(10014, message, data)


class DispatchFailedError(AppError):
    def __init__(self, message: str = "调度下发失败", data: Any = None) -> None:
        """
        创建调度下发失败异常，并固定使用错误码 20001。
        """
        super().__init__(20001, message, data)


class InvalidRmfCallbackError(AppError):
    def __init__(self, message: str = "RMF 回调数据非法", data: Any = None) -> None:
        """
        创建 RMF 回调数据非法异常，并固定使用错误码 20002。
        """
        super().__init__(20002, message, data)


class DatabaseError(AppError):
    def __init__(self, message: str = "数据库异常", data: Any = None) -> None:
        """
        创建数据库异常，并固定使用错误码 30001。
        """
        super().__init__(30001, message, data)


class RedisError(AppError):
    def __init__(self, message: str = "Redis 异常", data: Any = None) -> None:
        """
        创建 Redis 异常，并固定使用错误码 30002。
        """
        super().__init__(30002, message, data)


class InternalServerError(AppError):
    def __init__(self, message: str = "系统内部异常", data: Any = None) -> None:
        """
        创建系统内部异常，并固定使用错误码 50000。
        """
        super().__init__(50000, message, data)
