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


# 分数越低越先被消费，优先级 10 最高。
_PRIORITY_BUCKET = 1_000_000_000_000
_MAX_PRIORITY = 10

_ENQUEUE_SCRIPT = """
if redis.call('ZSCORE', KEYS[2], ARGV[1]) then
    return 0
end
if redis.call('ZSCORE', KEYS[3], ARGV[1]) then
    return 0
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('HSET', KEYS[4], ARGV[1], ARGV[2])
return 1
"""

_CLAIM_SCRIPT = """
local task_ids = redis.call('ZRANGE', KEYS[1], 0, 0)
if #task_ids == 0 then
    return nil
end
local task_id = task_ids[1]
redis.call('ZREM', KEYS[1], task_id)
redis.call('ZADD', KEYS[2], ARGV[1], task_id)
local attempts = redis.call('HINCRBY', KEYS[3], task_id, 1)
return {task_id, tostring(attempts)}
"""

_PROMOTE_RETRY_SCRIPT = """
local task_ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, task_id in ipairs(task_ids) do
    redis.call('ZREM', KEYS[1], task_id)
    local queue_score = redis.call('HGET', KEYS[3], task_id)
    if queue_score then
        redis.call('ZADD', KEYS[2], queue_score, task_id)
    end
end
return #task_ids
"""

_REQUEUE_EXPIRED_SCRIPT = """
local task_ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, task_id in ipairs(task_ids) do
    redis.call('ZREM', KEYS[1], task_id)
    redis.call('ZADD', KEYS[2], ARGV[1], task_id)
end
return #task_ids
"""


@dataclass(frozen=True)
class TaskClaim:
    task_id: int
    attempt: int


class TaskQueue:
    """API 和调度 Worker 共用的 Redis 队列操作。"""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    @staticmethod
    def score_for(priority: int, created_on: datetime | None = None) -> int:
        priority = max(1, min(_MAX_PRIORITY, int(priority)))
        created_ms = int((created_on or datetime.now()).timestamp() * 1000)
        return (_MAX_PRIORITY - priority) * _PRIORITY_BUCKET + created_ms

    async def enqueue(self, task_id: int, priority: int, created_on: datetime | None = None) -> bool:
        """将任务放入待调度池，任务正在处理时不重复入队。"""
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
        """原子地将下一个任务移动到处理中租约集合。"""
        del worker_id  # 当前暂时使用处理中 ZSET 表示租约。
        lease_until = time.time() + settings.scheduler_lease_seconds
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
        """数据库操作成功后，删除任务的全部队列元数据。"""
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
        """将失败任务移动到延迟重试池。"""
        member = str(task_id)
        retry_at = time.time() + delay_seconds
        pipeline = self.redis.pipeline(transaction=True)
        pipeline.zrem(TASK_PROCESSING_ZSET, member)
        pipeline.zadd(TASK_RETRY_ZSET, {member: retry_at})
        pipeline.hset(TASK_LAST_ERROR_HASH, member, error[:1000])
        await pipeline.execute()

    async def promote_due_retries(self, limit: int) -> int:
        result = await self.redis.eval(
            _PROMOTE_RETRY_SCRIPT,
            3,
            TASK_RETRY_ZSET,
            TASK_QUEUE_ZSET,
            TASK_QUEUE_SCORE_HASH,
            str(time.time()),
            str(limit),
        )
        return int(result or 0)

    async def requeue_expired(self, limit: int) -> int:
        """将处理中租约已过期的任务放回重试池。"""
        result = await self.redis.eval(
            _REQUEUE_EXPIRED_SCRIPT,
            2,
            TASK_PROCESSING_ZSET,
            TASK_RETRY_ZSET,
            str(time.time()),
            str(limit),
        )
        return int(result or 0)

    async def pending_count(self) -> int:
        return int(await self.redis.zcard(TASK_QUEUE_ZSET))

    async def processing_count(self) -> int:
        return int(await self.redis.zcard(TASK_PROCESSING_ZSET))
