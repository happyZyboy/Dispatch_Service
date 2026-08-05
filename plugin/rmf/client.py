from __future__ import annotations

import logging
from typing import Any


logger = logging.getLogger(__name__)


class RmfClient:
    def submit_block(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        向 RMF 模拟提交流程块任务，并返回提交结果。
        """
        logger.info("RMF 提交流程块任务：%s", payload)
        root_id = payload.get("rootBlockId") or payload.get("rootStepIndex") or "root"
        return {
            "success": True,
            "rmfTaskId": f"rmf-{payload.get('taskId')}-{root_id}",
            "payload": payload,
        }

    def report_progress(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        向 RMF 模拟上报流程块进度，并返回上报结果。
        """
        logger.info("RMF 上报流程块进度：%s", payload)
        return {"success": True, "payload": payload}

    def report_complete(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        向 RMF 模拟上报流程块完成事件，并返回上报结果。
        """
        logger.info("RMF 上报流程块完成：%s", payload)
        return {"success": True, "payload": payload}

    def report_failed(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        向 RMF 模拟上报流程块失败事件，并返回上报结果。
        """
        logger.info("RMF 上报流程块失败：%s", payload)
        return {"success": True, "payload": payload}
