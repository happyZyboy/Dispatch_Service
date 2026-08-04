from __future__ import annotations

from redis.asyncio import Redis

from core.conf import settings


_client: Redis | None = None


def get_redis() -> Redis:
    """Return the process-local async Redis client."""
    global _client
    if _client is None:
        _client = Redis.from_url(settings.redis_url, decode_responses=True)
    return _client


async def close_redis() -> None:
    """Close the process-local Redis connection pool."""
    global _client
    if _client is None:
        return
    await _client.aclose()
    _client = None
