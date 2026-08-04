from __future__ import annotations

import logging


def configure_logging() -> None:
    """
    配置项目统一的日志级别和日志输出格式。
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
