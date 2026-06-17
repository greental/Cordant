from fastapi import FastAPI
import redis.asyncio as redis
from motor.motor_asyncio import AsyncIOMotorClient

from app.config import settings
from app.cache import DependencyChainCache
from app.ingestion import TaskEventConsumer
from app.repositories import TaskRepository
from app.routes.tasks import router as tasks_router
from app.services import DependencyChainService


def create_app(
    *,
    mongodb_url: str | None = None,
    mongodb_database: str | None = None,
    redis_url: str | None = None,
    redis_key_prefix: str | None = None,
    kafka_bootstrap_servers: str | None = None,
    kafka_topic: str | None = None,
    kafka_consumer_group: str | None = None,
    enable_kafka_consumer: bool | None = None,
    dependency_chain_cache_enabled: bool | None = None,
    max_dependency_depth: int | None = None,
) -> FastAPI:
    """Create the FastAPI app with injectable infrastructure settings.

    Production uses defaults from environment-backed settings. Tests pass explicit
    Mongo database, Redis prefix, Kafka topic/group, and cache/consumer flags to
    keep integration runs isolated without mutating module-level singletons.
    """
    app = FastAPI(title="Task Dependency Service")

    effective_mongodb_url = mongodb_url or settings.mongodb_url
    effective_mongodb_database = mongodb_database or settings.mongodb_database
    effective_redis_url = redis_url or settings.redis_url
    effective_redis_key_prefix = (
        settings.redis_key_prefix if redis_key_prefix is None else redis_key_prefix
    )
    effective_cache_enabled = (
        settings.dependency_chain_cache_enabled
        if dependency_chain_cache_enabled is None
        else dependency_chain_cache_enabled
    )
    effective_kafka_bootstrap_servers = (
        kafka_bootstrap_servers or settings.kafka_bootstrap_servers
    )
    effective_kafka_topic = kafka_topic or settings.kafka_topic
    effective_kafka_consumer_group = kafka_consumer_group or settings.kafka_consumer_group
    effective_enable_kafka_consumer = (
        settings.enable_kafka_consumer
        if enable_kafka_consumer is None
        else enable_kafka_consumer
    )
    effective_max_depth = (
        max_dependency_depth
        or settings.max_dependency_depth
        or settings.dependency_chain_max_depth
    )

    mongo_client = AsyncIOMotorClient(effective_mongodb_url)
    collection = mongo_client[effective_mongodb_database]["tasks"]
    repository = TaskRepository(collection=collection)
    redis_client = redis.from_url(effective_redis_url, decode_responses=True)
    cache = DependencyChainCache(
        redis=redis_client,
        key_prefix=effective_redis_key_prefix,
        enabled=effective_cache_enabled,
    )
    service = DependencyChainService(
        repository=repository,
        cache=cache,
        max_depth=effective_max_depth,
    )
    consumer = TaskEventConsumer(
        repository=repository,
        cache=cache,
        topic=effective_kafka_topic,
        bootstrap_servers=effective_kafka_bootstrap_servers,
        group_id=effective_kafka_consumer_group,
    )

    app.state.mongo_client = mongo_client
    app.state.redis_client = redis_client
    app.state.task_repository = repository
    app.state.dependency_chain_cache = cache
    app.state.dependency_chain_service = service
    app.state.task_event_consumer = consumer
    app.state.enable_kafka_consumer = effective_enable_kafka_consumer

    app.include_router(tasks_router)

    async def startup() -> None:
        await repository.ensure_indexes()
        if app.state.enable_kafka_consumer:
            consumer.start()

    async def shutdown() -> None:
        await consumer.stop()
        await redis_client.aclose()
        mongo_client.close()

    app.router.on_startup.append(startup)
    app.router.on_shutdown.append(shutdown)

    return app


app = create_app()
