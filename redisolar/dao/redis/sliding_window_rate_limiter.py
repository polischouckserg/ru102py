from datetime import datetime
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        now = datetime.utcnow()
        ts = int(now.timestamp() * 1000)

        p = self.redis.pipeline()
        key = KeySchema().sliding_window_rate_limiter_key(name, int(self.window_size_ms), self.max_hits)
        p.zadd(key, mapping={f'{ts}-{name}': ts})
        p.zremrangebyscore(key, 0, ts - int(self.window_size_ms))
        p.zcard(key)

        res = p.execute()
        hits = res[2]

        if hits > self.max_hits:
            raise RateLimitExceededException
