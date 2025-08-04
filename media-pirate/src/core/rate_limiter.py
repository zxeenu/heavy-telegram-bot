

from redis import Redis
from time import time


class FixedWindowRateLimiter:
    def __init__(self, redis: Redis, window_seconds: int = 60, max_requests: int = 5):
        self.redis = redis
        self.window = window_seconds
        self.max_requests = max_requests

    def _get_redis_key(self, user_id: str) -> str:
        window_start = int(time() // self.window) * self.window
        return f"rate_limit:{user_id}:{window_start}"

    async def is_allowed(self, user_id: str) -> bool:
        redis_key = self._get_redis_key(user_id)
        current = await self.redis.get(redis_key)
        return (int(current) if current else 0) < self.max_requests

    async def increment(self, user_id: str):
        redis_key = self._get_redis_key(user_id)
        current = await self.redis.incr(redis_key)
        if current == 1:
            await self.redis.expire(redis_key, self.window)
        return current
