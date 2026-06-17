import json

import redis.asyncio as redis

from app.cache import TASKS_DATA_VERSION_KEY


class RedisTestClient:
    def __init__(self, redis_url: str, key_prefix: str):
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.client: redis.Redis | None = None

    async def start(self):
        self.client = redis.from_url(self.redis_url, decode_responses=True)

    async def stop(self):
        if self.client is not None:
            await self.client.aclose()

    def _key(self, key: str) -> str:
        return f"{self.key_prefix}{key}"

    async def clear_test_keys(self):
        cursor = 0
        pattern = f"{self.key_prefix}*"
        while True:
            cursor, keys = await self.client.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                await self.client.delete(*keys)
            if cursor == 0:
                break

    async def get_json(self, key: str) -> dict | None:
        raw = await self.client.get(self._key(key))
        return json.loads(raw) if raw is not None else None

    async def set_json(self, key: str, value: dict, ttl_seconds: int | None = None):
        payload = json.dumps(value)
        if ttl_seconds is None:
            await self.client.set(self._key(key), payload)
        else:
            await self.client.setex(self._key(key), ttl_seconds, payload)

    async def exists(self, key: str) -> bool:
        return bool(await self.client.exists(self._key(key)))

    async def ttl(self, key: str) -> int:
        return await self.client.ttl(self._key(key))

    async def get_data_version(self) -> int | None:
        value = await self.client.get(self._key(TASKS_DATA_VERSION_KEY))
        return int(value) if value is not None else None

    async def increment_data_version(self) -> int:
        return await self.client.incr(self._key(TASKS_DATA_VERSION_KEY))

    async def cache_keys_for_task(self, task_id: str) -> list[str]:
        key = self._key(f"task:{task_id}:dependency-chain")
        return [key] if await self.client.exists(key) else []

    async def get_dependency_chain_cache(self, task_id: str) -> dict | None:
        return await self.get_json(f"task:{task_id}:dependency-chain")

    async def assert_no_dependency_chain_cache(self, task_id: str):
        assert await self.get_dependency_chain_cache(task_id) is None

    async def assert_dependency_chain_cache_exists(self, task_id: str):
        assert await self.get_dependency_chain_cache(task_id) is not None