import sys
import os
import uuid
from dataclasses import dataclass
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.config import settings
from app.main import create_app
from tests.helpers.api_client import TaskApiClient
from tests.helpers.kafka_producer import KafkaTestProducer
from tests.helpers.mongo_test_client import MongoTestClient
from tests.helpers.redis_test_client import RedisTestClient
from tests.helpers.task_factory import TaskFactory


@dataclass(frozen=True)
class TestSettings:
    mongodb_url: str
    mongodb_database: str
    redis_url: str
    redis_prefix: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_consumer_group: str
    max_dependency_depth: int


@pytest.fixture
def run_id() -> str:
    return uuid.uuid4().hex[:12]


@pytest.fixture
def test_prefix(run_id: str) -> str:
    return f"test-{run_id}"


@pytest.fixture
def unique_test_topic(run_id: str) -> str:
    return f"tasks.events.test.{run_id}"


@pytest.fixture
def unique_consumer_group(run_id: str) -> str:
    return f"task-dependency-service-test-{run_id}"


@pytest.fixture
def test_settings(run_id: str, unique_test_topic: str, unique_consumer_group: str) -> TestSettings:
    return TestSettings(
        mongodb_url=os.getenv("MONGODB_URL", settings.mongodb_url),
        mongodb_database=os.getenv("TEST_MONGODB_DATABASE", settings.test_mongodb_database),
        redis_url=os.getenv("REDIS_URL", settings.redis_url),
        redis_prefix=os.getenv("TEST_REDIS_PREFIX", f"test:{run_id}:"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", settings.kafka_bootstrap_servers),
        kafka_topic=os.getenv("KAFKA_TOPIC", unique_test_topic),
        kafka_consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", unique_consumer_group),
        max_dependency_depth=int(os.getenv("MAX_DEPENDENCY_DEPTH", str(settings.max_dependency_depth or settings.dependency_chain_max_depth))),
    )


@pytest.fixture
def task_factory() -> type[TaskFactory]:
    return TaskFactory


@pytest.fixture
async def mongo_test_client(test_settings: TestSettings) -> AsyncIterator[MongoTestClient]:
    client = MongoTestClient(test_settings.mongodb_url, test_settings.mongodb_database)
    await client.start()
    try:
        yield client
    finally:
        await client.stop()


@pytest.fixture
async def redis_test_client(test_settings: TestSettings) -> AsyncIterator[RedisTestClient]:
    client = RedisTestClient(test_settings.redis_url, test_settings.redis_prefix)
    await client.start()
    try:
        yield client
    finally:
        await client.stop()


@pytest.fixture
async def kafka_test_producer(test_settings: TestSettings) -> AsyncIterator[KafkaTestProducer]:
    producer = KafkaTestProducer(test_settings.kafka_bootstrap_servers, test_settings.kafka_topic)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@pytest.fixture
async def clean_datastores(mongo_test_client: MongoTestClient, redis_test_client: RedisTestClient):
    await mongo_test_client.clear_tasks()
    await redis_test_client.clear_test_keys()
    yield
    await mongo_test_client.clear_tasks()
    await redis_test_client.clear_test_keys()


async def _make_api_client(test_settings: TestSettings, *, enable_kafka_consumer: bool, cache_enabled: bool = True):
    app = create_app(
        mongodb_url=test_settings.mongodb_url,
        mongodb_database=test_settings.mongodb_database,
        redis_url=test_settings.redis_url,
        redis_key_prefix=test_settings.redis_prefix,
        kafka_bootstrap_servers=test_settings.kafka_bootstrap_servers,
        kafka_topic=test_settings.kafka_topic,
        kafka_consumer_group=test_settings.kafka_consumer_group,
        enable_kafka_consumer=enable_kafka_consumer,
        dependency_chain_cache_enabled=cache_enabled,
        max_dependency_depth=test_settings.max_dependency_depth,
    )
    await app.router.startup()
    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://test")
    return app, client


@pytest.fixture
async def running_app_without_kafka(test_settings: TestSettings, clean_datastores):
    app, client = await _make_api_client(test_settings, enable_kafka_consumer=False)
    try:
        yield app, TaskApiClient(client)
    finally:
        await client.aclose()
        await app.router.shutdown()


@pytest.fixture
async def running_app_with_kafka(test_settings: TestSettings, clean_datastores):
    app, client = await _make_api_client(test_settings, enable_kafka_consumer=True)
    try:
        yield app, TaskApiClient(client)
    finally:
        await client.aclose()
        await app.router.shutdown()


@pytest.fixture
async def task_api_client(running_app_without_kafka):
    _, client = running_app_without_kafka
    return client