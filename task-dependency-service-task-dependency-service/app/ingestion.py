import asyncio
import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from app.cache import DependencyChainCache, dependency_chain_cache
from app.config import settings
from app.models import Task
from app.repositories import TaskRepository, task_repository

logger = logging.getLogger(__name__)


class TaskEvent(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)

    id: str
    title: str
    parent_task_id: str | None = None

    @field_validator("id", "title")
    @classmethod
    def non_blank_required_string(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value

    @field_validator("parent_task_id")
    @classmethod
    def non_blank_optional_parent(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("parent_task_id must not be blank")
        return value

    def to_task(self) -> Task:
        return Task(id=self.id, title=self.title, parent_task_id=self.parent_task_id)


async def handle_task_event(
    payload: bytes,
    repository: TaskRepository = task_repository,
    cache: DependencyChainCache = dependency_chain_cache,
) -> bool:
    """Persist one task event.

    Returns True when the Kafka offset may be committed. Invalid events are
    commit-safe poison messages; persistence errors are not commit-safe and are
    intentionally allowed to propagate.
    """
    try:
        raw_event: Any = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.warning("Skipping invalid task event JSON", exc_info=True)
        return True

    try:
        event = TaskEvent.model_validate(raw_event)
    except ValidationError:
        logger.warning("Skipping invalid task event shape: %s", raw_event, exc_info=True)
        return True

    await repository.upsert_task(event.to_task())
    await cache.bump_data_version()
    return True


class TaskEventConsumer:
    def __init__(
        self,
        repository: TaskRepository = task_repository,
        cache: DependencyChainCache = dependency_chain_cache,
        topic: str = settings.kafka_topic,
        bootstrap_servers: str = settings.kafka_bootstrap_servers,
        group_id: str = settings.kafka_consumer_group,
    ):
        self.repository = repository
        self.cache = cache
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stopping.clear()
            self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run_forever(self) -> None:
        while not self._stopping.is_set():
            consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
            try:
                await consumer.start()
                logger.info("Kafka consumer started for topic %s", self.topic)
                async for message in consumer:
                    try:
                        should_commit = await handle_task_event(
                            message.value, self.repository, self.cache
                        )
                    except Exception:
                        logger.exception("Task event persistence failed; offset will not be committed")
                        continue

                    if should_commit:
                        try:
                            await consumer.commit()
                        except Exception:
                            logger.exception("Kafka offset commit failed after task event processing")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Kafka consumer unavailable; retrying soon")
                await asyncio.sleep(5)
            finally:
                try:
                    await consumer.stop()
                except Exception:
                    logger.exception("Failed to stop Kafka consumer cleanly")


task_event_consumer = TaskEventConsumer()