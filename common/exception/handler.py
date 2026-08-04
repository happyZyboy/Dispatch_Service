from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from common.exception.base import AppError
from common.response.response_schema import failure


def register_exception_handlers(app: FastAPI) -> None:
    """
    注册全局异常处理器。
    """

    @app.exception_handler(AppError)
    async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
        """
        将项目自定义业务异常转换成统一格式的 JSON 响应。
        """
        # 业务异常：说明业务规则没通过，比如站点不存在、任务状态不允许等。
        return JSONResponse(status_code=200, content=failure(exc.code, exc.message, exc.data).model_dump())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        """
        将 FastAPI 请求参数校验异常转换成统一的参数错误响应。
        """
        # FastAPI 参数校验失败统一映射到 10011。
        return JSONResponse(status_code=200, content=failure(10011, "参数校验失败", exc.errors()).model_dump())

    @app.exception_handler(Exception)
    async def generic_error_handler(_: Request, exc: Exception) -> JSONResponse:
        """
        捕获未分类异常并返回统一的系统内部错误响应。
        """
        # 未知异常统一走 50000，避免把 Python 堆栈直接暴露给前端。
        return JSONResponse(status_code=500, content=failure(50000, "系统内部异常", str(exc)).model_dump())
