from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime

from redis.asyncio import Redis

from common.constant.redis_key import (
    TASK_ATTEMPTS_HASH,
    TASK_LAST_ERROR_HASH,
    TASK_PROCESSING_ZSET,
    TASK_QUEUE_SCORE_HASH,
    TASK_QUEUE_ZSET,
    TASK_RETRY_ZSET,
)
from core.conf import settings
from scheduler.lua import (
    _CLAIM_SCRIPT,
    _ENQUEUE_SCRIPT,
    _PROMOTE_RETRY_SCRIPT,
    _REQUEUE_EXPIRED_SCRIPT,
)


# 分数越低越先被消费，优先级 10 最高。
_PRIORITY_BUCKET = 1_000_000_000_000
_MAX_PRIORITY = 10

@dataclass(frozen=True)
class TaskClaim:
    """记录 Worker 从 Redis 队列中领取到的任务信息。"""

    task_id: int
    attempt: int #尝试次数


class TaskQueue:
    """API 和调度 Worker 共用的 Redis 队列操作。"""

    def __init__(self, redis: Redis) -> None:
        """
        初始化任务队列操作对象。

        :param redis: 已创建的异步 Redis 客户端。
        """
        self.redis = redis

    @staticmethod
    def score_for(priority: int, created_on: datetime | None = None) -> int:
        """
        根据任务优先级和创建时间生成有序集合分数。

        分数越小越先被领取；优先级越高，优先级分段分数越小；
        同一优先级下按照任务创建时间先后处理。

        :param priority: 任务优先级，最终会限制在 1 到 10 之间。
        :param created_on: 任务创建时间，不传时使用当前时间。
        :return: 用于 Redis Sorted Set 排序的整数分数。
        """
        priority = max(1, min(_MAX_PRIORITY, int(priority)))
        created_ms = int((created_on or datetime.now()).timestamp() * 1000)
        return (_MAX_PRIORITY - priority) * _PRIORITY_BUCKET + created_ms

    async def enqueue(self, task_id: int, priority: int, created_on: datetime | None = None) -> bool:
        """
        将任务放入待调度池，并避免任务重复入队。

        Lua 脚本会原子检查任务是否已经处于处理中或延迟重试状态，
        只有两个集合中都不存在时才会写入待调度有序集合。

        :param task_id: 要进入调度队列的任务主键。
        :param priority: 任务优先级。
        :param created_on: 任务创建时间，用于同优先级任务排序。
        :return: 成功入队返回 True；任务已在其他队列中时返回 False。
        """
        member = str(task_id)
        score = self.score_for(priority, created_on)
        result = await self.redis.eval(
            _ENQUEUE_SCRIPT,
            4,
            TASK_QUEUE_ZSET,
            TASK_PROCESSING_ZSET,
            TASK_RETRY_ZSET,
            TASK_QUEUE_SCORE_HASH,
            member,
            str(score),
        )
        return bool(result)

    async def claim(self, worker_id: str) -> TaskClaim | None:
        """
        原子地领取一个待调度任务并创建处理租约。

        Lua 脚本会从待调度有序集合取出队首任务，移动到处理中集合，
        同时递增该任务的调度尝试次数。

        :param worker_id: 当前 Worker 标识，暂时只保留该参数用于后续租约扩展。
        :return: 领取成功时返回任务主键和尝试次数；队列为空时返回 None。
        """
        del worker_id  # 当前暂时使用处理中 ZSET 表示租约。
        lease_until = int(time.time()) + settings.scheduler_lease_seconds   #设置任务的"租约到期时间"
        result = await self.redis.eval(
            _CLAIM_SCRIPT,
            3,
            TASK_QUEUE_ZSET,
            TASK_PROCESSING_ZSET,
            TASK_ATTEMPTS_HASH,
            str(lease_until),
        )
        if not result:
            return None
        return TaskClaim(task_id=int(result[0]), attempt=int(result[1]))

    async def ack(self, task_id: int) -> None:
        """
        确认任务处理完成并清理其 Redis 队列元数据。

        :param task_id: 已成功处理的任务主键。
        """
        member = str(task_id)
        pipeline = self.redis.pipeline(transaction=True)
        pipeline.zrem(TASK_QUEUE_ZSET, member)
        pipeline.zrem(TASK_PROCESSING_ZSET, member)
        pipeline.zrem(TASK_RETRY_ZSET, member)
        pipeline.hdel(TASK_QUEUE_SCORE_HASH, member)
        pipeline.hdel(TASK_ATTEMPTS_HASH, member)
        pipeline.hdel(TASK_LAST_ERROR_HASH, member)
        await pipeline.execute()

    async def schedule_retry(self, task_id: int, delay_seconds: int, error: str) -> None:
        """
        将处理失败的任务放入延迟重试池，并记录最近一次错误。

        :param task_id: 处理失败的任务主键。
        :param delay_seconds: 下次允许重试前需要等待的秒数。
        :param error: 本次失败原因，最多保存 1000 个字符。
        """
        member = str(task_id)
        retry_at = int(time.time()) + delay_seconds
        pipeline = self.redis.pipeline(transaction=True)
        pipeline.zrem(TASK_PROCESSING_ZSET, member)
        pipeline.zadd(TASK_RETRY_ZSET, {member: retry_at})
        pipeline.hset(TASK_LAST_ERROR_HASH, member, error[:1000])
        await pipeline.execute()

    async def promote_due_retries(self, limit: int) -> int:
        """
        将到达重试时间的任务提升回待调度队列。

        :param limit: 本次最多处理的重试任务数量。
        :return: 本次重新放回待调度队列的任务数量。
        """
        result = await self.redis.eval(
            _PROMOTE_RETRY_SCRIPT,
            3,
            TASK_RETRY_ZSET,
            TASK_QUEUE_ZSET,
            TASK_QUEUE_SCORE_HASH,
            str(int(time.time())),
            str(limit),
        )
        return int(result or 0)

    async def requeue_expired(self, limit: int) -> int:
        """
        将处理中租约已过期的任务放回延迟重试池。

        :param limit: 本次最多回收的过期任务数量。
        :return: 本次回收并重新排队的任务数量。
        """
        result = await self.redis.eval(
            _REQUEUE_EXPIRED_SCRIPT,
            2,
            TASK_PROCESSING_ZSET,
            TASK_RETRY_ZSET,
            str(int(time.time())),
            str(limit),
        )
        return int(result or 0)

    async def pending_count(self) -> int:
        """
        统计待调度队列中的任务数量。

        :return: 待调度有序集合中的任务数量。
        """
        return int(await self.redis.zcard(TASK_QUEUE_ZSET))

    async def processing_count(self) -> int:
        """
        统计当前处于处理租约中的任务数量。

        :return: 处理中有序集合中的任务数量。
        """
        return int(await self.redis.zcard(TASK_PROCESSING_ZSET))
