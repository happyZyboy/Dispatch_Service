from __future__ import annotations

import logging
from typing import Any


logger = logging.getLogger(__name__)


class WmsClient:
    def notify_task_change(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        向 WMS 模拟通知任务状态变化，并返回通知结果。
        """
        logger.info("wms notify_task_change: %s", payload)
        return {"success": True, "payload": payload}
