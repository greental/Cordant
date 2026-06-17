import json
import logging
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from app.config import settings
from app.db import redis_client
from app.models import CachedDependencyChainResponse, DependencyChainResponse

logger = logging.getLogger(__name__)

TASKS_DATA_VERSION_KEY = "tasks:data-version"


@dataclass(frozen=True)
class DependencyChainCacheLookup:
    status: str
    response: DependencyChainResponse | None = None


class DependencyChainCache:
    def __init__(self, redis=redis_client, key_prefix: str | None = None, enabled: bool | None = None):
        self.redis = redis
        self.key_prefix = settings.redis_key_prefix if key_prefix is None else key_prefix
        self.enabled = settings.dependency_chain_cache_enabled if enabled is None else enabled

    def _key(self, key: str) -> str:
        return f"{self.key_prefix}{key}"

    def _chain_key(self, task_id: str) -> str:
        return self._key(f"task:{task_id}:dependency-chain")

    def _version_key(self) -> str:
        return self._key(TASKS_DATA_VERSION_KEY)

    async def get_data_version(self) -> int | None:
        if not self.enabled:
            return None
        try:
            value = await self.redis.get(self._version_key())
        except Exception:
            logger.warning("Redis data-version read failed; skipping cache", exc_info=True)
            return None

        if value is None:
            return 0

        try:
            return int(value)
        except ValueError:
            logger.warning("Redis data-version value is invalid: %s", value)
            return None

    async def bump_data_version(self) -> None:
        if not self.enabled:
            return
        try:
            await self.redis.incr(self._version_key())
        except Exception:
            logger.warning("Redis data-version increment failed", exc_info=True)

    async def get_chain_lookup(
        self,
        task_id: str,
        current_version: int | None = None,
    ) -> DependencyChainCacheLookup:
        if not self.enabled:
            return DependencyChainCacheLookup(status="bypass")
        if current_version is None:
            current_version = await self.get_data_version()
        if current_version is None:
            return DependencyChainCacheLookup(status="bypass")

        try:
            raw_payload = await self.redis.get(self._chain_key(task_id))
        except Exception:
            logger.warning("Redis dependency-chain read failed", exc_info=True)
            return DependencyChainCacheLookup(status="bypass")

        if raw_payload is None:
            return DependencyChainCacheLookup(status="miss")

        try:
            payload: Any = json.loads(raw_payload)
            cached = CachedDependencyChainResponse.model_validate(payload)
        except (json.JSONDecodeError, ValidationError):
            logger.warning("Cached dependency-chain payload is invalid", exc_info=True)
            return DependencyChainCacheLookup(status="miss")

        if cached.data_version != current_version:
            return DependencyChainCacheLookup(status="stale")
        if not cached.response.complete or cached.response.warnings:
            return DependencyChainCacheLookup(status="miss")
        return DependencyChainCacheLookup(status="hit", response=cached.response)

    async def get_chain(self, task_id: str) -> DependencyChainResponse | None:
        lookup = await self.get_chain_lookup(task_id)
        return lookup.response

    async def set_chain(self, response: DependencyChainResponse) -> None:
        if not self.enabled:
            return
        if not response.complete or response.warnings:
            return

        current_version = await self.get_data_version()
        if current_version is None:
            return

        cached = CachedDependencyChainResponse(
            data_version=current_version,
            response=response,
        )
        try:
            await self.redis.set(
                self._chain_key(response.task_id),
                cached.model_dump_json(),
                ex=settings.dependency_chain_cache_ttl_seconds,
            )
        except Exception:
            logger.warning("Redis dependency-chain write failed", exc_info=True)


dependency_chain_cache = DependencyChainCache()