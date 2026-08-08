from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from sqlalchemy import select

from app.domain import (
    create_task_log,
    mark_robot_idle,
    release_reserved_map_nodes,
)
from app.map.service import get_active_map_version, run_map_cache_listener
from common.enums.block_status import BlockStatus
from common.enums.task_status import TaskStatus
from common.utils import from_json_text, now, to_json_text
from core.conf import settings
from database.db import SessionLocal
from database.models import WindBlockRecord, WindTaskRecord
from plugin.rmf.client import RmfClient
from scheduler.rabbitmq import close_rabbitmq, get_rabbitmq, publish_rmf_dispatch


logger = logging.getLogger(__name__)
rmf_client = RmfClient()


class StaleDispatchMessage(Exception):
    """消息已经不再代表当前任务分配结果。"""


class RmfDispatchError(Exception):
    """下游 RMF 调用失败，可以继续重试。"""


class RmfDispatchConsumer:
    """从 RabbitMQ 消费调度消息并向 RMF 提交单条操作块。"""

    def __init__(self) -> None:
        """
        初始化 RMF 调度消息消费者。

        消费者复用全局 RabbitMQ 客户端，实际连接和队列拓扑在首次使用时建立。
        """
        self.rabbitmq = get_rabbitmq()

    async def run(self) -> None:
        """
        启动 RabbitMQ 消费循环。

        每条消息都会交给 ``_handle_message`` 处理，成功消息确认，
        可重试失败消息重新投递，过期或不可恢复消息进入死信流程。
        """
        queue = await self.rabbitmq.get_queue()
        logger.info("RMF 调度消费者已启动：队列=%s", settings.rabbitmq_queue)
        async with queue.iterator() as messages:
            async for message in messages:
                await self._handle_message(message)

    async def _handle_message(self, message: Any) -> None:
        """
        解析并处理一条 RabbitMQ 调度消息。

        :param message: aio-pika 提供的消息对象。
        """
        payload: dict[str, Any] | None = None
        try:
            payload = json.loads(message.body.decode("utf-8"))
            await self._dispatch(payload)
        except (ValueError, KeyError, StaleDispatchMessage) as exc:
            logger.warning("丢弃过期或无效的 RMF 消息：%s", exc)
            await message.reject(requeue=False)
            return
        except Exception as exc:
            attempt = self._message_attempt(message)
            if payload is not None and attempt < settings.rabbitmq_max_retries:
                try:
                    await publish_rmf_dispatch(payload, attempt=attempt + 1)
                    await message.ack()
                    logger.warning(
                        "RMF 下发失败，消息已安排重试：task_id=%s attempt=%s",
                        payload.get("taskId"),
                        attempt + 1,
                    )
                    return
                except Exception:
                    logger.exception("重新投递 RMF 消息失败")
                    await message.nack(requeue=True)
                    return

            if payload is not None:
                await self._mark_failed(payload, str(exc))
            logger.exception("RMF 下发重试次数已耗尽：task_id=%s", (payload or {}).get("taskId"))
            await message.reject(requeue=False)
            return

        await message.ack()

    async def _dispatch(self, payload: dict[str, Any]) -> None:
        """
        校验调度消息并把数据库中已创建的单条操作块提交给 RMF。

        :param payload: 调度服务投递到 RabbitMQ 的任务分配消息。
        :return: 无返回值；RMF 接收结果会写回任务状态。
        """
        task_id = int(payload["taskId"])
        agv_id = str(payload["agvId"])
        dispatch_key = str(payload["dispatchKey"])
        root_block_id = str(payload.get("rootBlockId") or "")
        claim = await self._claim_task(
            task_id,
            agv_id,
            dispatch_key,
            root_block_id,
            str(payload.get("mapVersionId") or ""),
            payload.get("operations") or [],
        )
        if claim is None:
            return

        try:
            result = await asyncio.to_thread(rmf_client.submit_block, claim)
            if not result or result.get("success") is False:
                raise RmfDispatchError(f"RMF 拒绝任务：{task_id}")
        except Exception as exc:
            await self._reset_for_retry(task_id, str(exc), claim.get("rootBlockId"))
            raise

        await self._mark_dispatched(task_id, dispatch_key, result)

    async def _claim_task(
        self,
        task_id: int,
        agv_id: str,
        dispatch_key: str,
        requested_root_id: str,
        requested_map_version_id: str,
        planned_operations: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """
        在事务中认领当前任务和单条动作块，防止同一调度消息被重复提交。

        :param task_id: 任务主键。
        :param agv_id: 消息指定的机器人编码。
        :param dispatch_key: 本次调度投递的幂等键。
        :param requested_root_id: 消息指定的 RootBp 编码。
        :param requested_map_version_id: 消息绑定的地图版本编码。
        :param planned_operations: 消息携带的当前操作块描述列表，正常情况下只有一条。
        :return: 可提交给 RMF 的单步任务载荷；消息已经过期时返回 None。
        """
        async with SessionLocal() as db:
            task = await db.scalar(
                select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update()
            )
            if not task:
                raise StaleDispatchMessage(f"任务不存在：{task_id}")
            if task.status == TaskStatus.DISPATCHED:
                return None
            if task.status not in {TaskStatus.ASSIGNED, TaskStatus.DISPATCHING}:
                raise StaleDispatchMessage(f"任务状态不可下发：{task.status}")
            if task.agv_id != agv_id:
                raise StaleDispatchMessage(f"机器人分配已变更：task_id={task_id}")
            if requested_map_version_id and str(task.map_version_id) != requested_map_version_id:
                raise StaleDispatchMessage(f"地图版本已变更：task_id={task_id}")

            active_map_version = await get_active_map_version(db)
            if task.map_version_id != active_map_version.id:
                variables = from_json_text(task.variables, {})
                task.status = TaskStatus.SUSPENDED
                task.ended_reason = (
                    f"任务绑定的地图版本已失效，taskMapVersionId={task.map_version_id}, "
                    f"activeMapVersionId={active_map_version.id}"
                )
                if task.agv_id:
                    await mark_robot_idle(db, task.agv_id, variables.get("currentSite"))
                    await release_reserved_map_nodes(db, task, task.agv_id)
                create_task_log(db, task.id, task.ended_reason, level="ERROR")
                await db.commit()
                return None

            variables = from_json_text(task.variables, {})
            if variables.get("rmfDispatchKey") != dispatch_key:
                raise StaleDispatchMessage(f"调度键已变更：task_id={task_id}")
            if task.status == TaskStatus.DISPATCHING:
                started_at = float(variables.get("rmfDispatchStartedAt") or 0)
                if int(time.time()) - started_at < settings.rabbitmq_dispatch_lease_seconds:
                    return None

            # 当前有效的 RootBp 是任务中唯一需要发送给 RMF 的部分。
            root = await db.scalar(
                select(WindBlockRecord)
                .where(
                    WindBlockRecord.task_record_id == task.id,
                    WindBlockRecord.block_name == "RootBp",
                    WindBlockRecord.status.in_([BlockStatus.CREATED, BlockStatus.RUNNING]),
                )
                .order_by(WindBlockRecord.id.desc())
            )
            if not root:
                raise StaleDispatchMessage(f"没有可用的 RootBp：task_id={task_id}")
            if requested_root_id and root.block_id != requested_root_id:
                raise StaleDispatchMessage(f"RootBp 已变更：task_id={task_id}")
            # 操作块已经在调度阶段落库，消费端只负责认领和发送，不能再次批量创建。
            operation_payload = planned_operations[0] if planned_operations else {}
            operation_block_id = str(operation_payload.get("blockId") or "")
            operation_order_id = str(operation_payload.get("orderId") or "")
            operation_stmt = select(WindBlockRecord).where(
                WindBlockRecord.task_record_id == task.id,
                WindBlockRecord.parent_block_id == root.block_id,
                WindBlockRecord.block_name == "CAgvOperationBp",
            )
            if operation_block_id:
                operation_stmt = operation_stmt.where(
                    WindBlockRecord.block_id == operation_block_id
                )
            elif operation_order_id:
                operation_stmt = operation_stmt.where(
                    WindBlockRecord.order_id == operation_order_id
                )
            else:
                operation_stmt = operation_stmt.where(
                    WindBlockRecord.status.in_([BlockStatus.CREATED, BlockStatus.RUNNING])
                ).order_by(WindBlockRecord.id.asc())
            operation_block = await db.scalar(operation_stmt)
            if operation_block is None:
                raise StaleDispatchMessage(f"RootBp 没有可用的动作块：task_id={task_id}")
            if operation_block.status == BlockStatus.SUCCESS:
                raise StaleDispatchMessage(f"动作块已完成：task_id={task_id}")

            operation_input = from_json_text(operation_block.block_input_params_value, {})
            operation_segment = {
                "from": operation_input.get("from"),
                "to": operation_input.get("to"),
                "segmentType": operation_input.get("segmentType", "map"),
                "startPose": operation_input.get("startPose"),
                "stepIndex": int(
                    from_json_text(operation_block.internal_variables, {}).get("stepIndex") or 1
                ),
            }
            root.status = BlockStatus.RUNNING
            root.started_on = root.started_on or now()
            active_root_id = root.block_id
            path_snapshot = from_json_text(task.path, {})

            task.status = TaskStatus.DISPATCHING
            variables["rmfDispatchStartedAt"] = int(time.time())
            task.variables = to_json_text(variables)
            create_task_log(db, task.id, "已领取 RMF 下发任务")
            await db.commit()
            return {
                "taskId": task.id,
                "agvId": agv_id,
                "dispatchKey": dispatch_key,
                "rootBlockId": active_root_id,
                "mapVersionId": str(task.map_version_id) if task.map_version_id else None,
                "entryNode": path_snapshot.get("entryNode"),
                "startPose": path_snapshot.get("startPose"),
                "root": {
                    "from": operation_segment["from"],
                    "to": operation_segment["to"],
                },
                "segments": [operation_segment],
                "rootStepIndex": from_json_text(root.internal_variables, {}).get("rootStepIndex"),
                "blocks": [
                    {
                        "blockId": operation_block.block_id,
                        "parentBlockId": operation_block.parent_block_id,
                        "blockName": operation_block.block_name,
                        "orderId": operation_block.order_id,
                        "inputParams": operation_input,
                        "internalVariables": from_json_text(operation_block.internal_variables, {}),
                    }
                ],
            }

    async def _mark_dispatched(self, task_id: int, dispatch_key: str, result: dict[str, Any]) -> None:
        """
        将 RMF 已成功接收的任务更新为已下发状态。

        :param task_id: 任务主键。
        :param dispatch_key: 本次调度投递的幂等键。
        :param result: RMF 返回的结果载荷。
        """
        async with SessionLocal() as db:
            task = await db.scalar(
                select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update()
            )
            if not task or task.status == TaskStatus.DISPATCHED:
                return

            variables = from_json_text(task.variables, {})
            if variables.get("rmfDispatchKey") != dispatch_key:
                raise StaleDispatchMessage(f"RMF 提交前调度键已变更：task_id={task_id}")
            rmf_task_id = self._extract_rmf_task_id(result)
            if rmf_task_id is not None:
                variables["rmfTaskId"] = rmf_task_id
            variables.pop("rmfDispatchStartedAt", None)
            variables["rmfPublished"] = True
            task.variables = to_json_text(variables)
            task.status = TaskStatus.DISPATCHED
            create_task_log(db, task.id, "RMF 已接受任务下发")
            await db.commit()

    async def _reset_for_retry(self, task_id: int, error: str, root_block_id: str | None) -> None:
        """
        清理本次 RMF 下发租约并恢复任务，使其可以重新投递。

        :param task_id: 任务主键。
        :param error: 本次下发失败原因。
        :param root_block_id: 本次投递对应的 RootBp 编码。
        """
        async with SessionLocal() as db:
            task = await db.scalar(
                select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update()
            )
            if not task or task.status == TaskStatus.DISPATCHED:
                return
            variables = from_json_text(task.variables, {})
            variables.pop("rmfDispatchStartedAt", None)
            task.variables = to_json_text(variables)
            task.status = TaskStatus.ASSIGNED
            if root_block_id:
                root = await db.scalar(
                    select(WindBlockRecord).where(
                        WindBlockRecord.task_record_id == task.id,
                        WindBlockRecord.block_id == root_block_id,
                    )
                )
                if root and root.status == BlockStatus.RUNNING:
                    root.status = BlockStatus.CREATED
                    root.started_on = None
            task.ended_reason = error[:500]
            create_task_log(db, task.id, f"RMF 下发重试：{error[:500]}")
            await db.commit()

    async def _mark_failed(self, payload: dict[str, Any], error: str) -> None:
        """
        在 RMF 重试次数耗尽后，将任务和当前 RootBp 标记为失败。

        :param payload: 原始调度消息载荷。
        :param error: 最终失败原因。
        """
        task_id = int(payload["taskId"])
        async with SessionLocal() as db:
            task = await db.scalar(
                select(WindTaskRecord).where(WindTaskRecord.id == task_id).with_for_update()
            )
            if not task or task.status == TaskStatus.DISPATCHED:
                return
            variables = from_json_text(task.variables, {})
            current_site = variables.get("currentSite")
            task.status = TaskStatus.FAILED
            task.ended_on = now()
            task.ended_reason = f"RMF 下发重试次数已耗尽：{error[:500]}"
            root_block_id = str(payload.get("rootBlockId") or "")
            if root_block_id:
                root = await db.scalar(
                    select(WindBlockRecord).where(
                        WindBlockRecord.task_record_id == task.id,
                        WindBlockRecord.block_id == root_block_id,
                    )
                )
                if root:
                    root.status = BlockStatus.FAILED
                    root.ended_on = now()
                    root.ended_reason = error[:500]
            if task.agv_id:
                await mark_robot_idle(db, task.agv_id, current_site)
                await release_reserved_map_nodes(db, task, task.agv_id)
            create_task_log(db, task.id, task.ended_reason, level="ERROR")
            await db.commit()

    @staticmethod
    def _message_attempt(message: Any) -> int:
        """
        读取 RabbitMQ 消息头中的当前重试次数。

        :param message: aio-pika 消息对象。
        :return: 当前已尝试投递次数，没有消息头时返回 0。
        """
        headers = message.headers or {}
        return int(headers.get("x-attempt", 0))

    @staticmethod
    def _extract_rmf_task_id(result: dict[str, Any]) -> Any:
        """
        从 RMF 响应的常见字段位置提取 RMF 任务编号。

        :param result: RMF 返回的结果载荷。
        :return: RMF 任务编号；响应中不存在时返回 None。
        """
        return (
            result.get("rmfTaskId")
            or result.get("taskId")
            or (result.get("payload") or {}).get("rmfTaskId")
        )


async def run_rmf_consumer() -> None:
    """
    以独立进程方式启动 RMF RabbitMQ 消费者。

    进程退出或发生取消时，会停止地图缓存监听并关闭 RabbitMQ 连接。
    """
    stop_event = asyncio.Event()
    map_cache_task = asyncio.create_task(
        run_map_cache_listener(stop_event),
        name="rmf-consumer-map-cache-listener",
    )
    try:
        await RmfDispatchConsumer().run()
    finally:
        stop_event.set()
        map_cache_task.cancel()
        try:
            await map_cache_task
        except asyncio.CancelledError:
            pass
        await close_rabbitmq()
