import pytest

from app.main import create_app
from tests.helpers.api_client import TaskApiClient
from tests.helpers.metrics import print_test_summary
from tests.helpers.polling import wait_until


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_valid_task_event_is_consumed_persisted_and_readable(kafka_test_producer, mongo_test_client, running_app_with_kafka):
    _, api = running_app_with_kafka
    await kafka_test_producer.send_event({"id": "kafka-valid", "title": "Kafka valid"})
    await mongo_test_client.wait_for_task("kafka-valid", 15)
    await api.assert_task_exists("kafka-valid")
    print_test_summary("Kafka valid event", {
        "flow": "AIOKafkaProducer -> Kafka broker -> app consumer -> Mongo -> API GET",
        "task_id": "kafka-valid",
        "persisted": True,
        "api_readable": True,
    })


@pytest.mark.asyncio
async def test_duplicate_replayed_kafka_event_is_idempotent(kafka_test_producer, mongo_test_client, running_app_with_kafka):
    event = {"id": "kafka-dupe", "title": "Kafka dupe"}
    await kafka_test_producer.send_events([event, event, event])
    await mongo_test_client.wait_for_task("kafka-dupe", 15)
    assert await mongo_test_client.count_by_id("kafka-dupe") == 1
    print_test_summary("Kafka duplicate replay", {
        "events_sent_for_same_id": 3,
        "mongo_documents_for_id": await mongo_test_client.count_by_id("kafka-dupe"),
        "proof": "Mongo upsert by id makes Kafka replay idempotent",
    })


@pytest.mark.asyncio
async def test_same_id_update_from_kafka(kafka_test_producer, mongo_test_client, running_app_with_kafka):
    _, api = running_app_with_kafka
    await kafka_test_producer.send_event({"id": "kafka-update", "title": "Old", "parent_task_id": None})
    await mongo_test_client.wait_for_task("kafka-update", 15)
    await kafka_test_producer.send_event({"id": "kafka-update", "title": "New", "parent_task_id": "parent"})

    async def updated() -> bool:
        doc = await mongo_test_client.get_task("kafka-update")
        return doc is not None and doc["title"] == "New" and doc["parent_task_id"] == "parent"

    await wait_until(updated, 15, description="Kafka update visible in Mongo")
    body = await api.assert_task_exists("kafka-update")
    assert body["title"] == "New"
    print_test_summary("Kafka same-id update", {
        "task_id": "kafka-update",
        "final_title": body["title"],
        "final_parent_task_id": body["parent_task_id"],
        "proof": "later event for same id updated existing Mongo document",
    })


@pytest.mark.asyncio
async def test_out_of_order_arrival_completes_after_parent_arrives(kafka_test_producer, mongo_test_client, running_app_with_kafka):
    _, api = running_app_with_kafka
    await kafka_test_producer.send_event({"id": "late-child", "title": "Child", "parent_task_id": "late-parent"})
    await mongo_test_client.wait_for_task("late-child", 15)
    await api.assert_chain_incomplete("late-child", "missing_parent")
    await kafka_test_producer.send_event({"id": "late-parent", "title": "Parent"})
    await mongo_test_client.wait_for_task("late-parent", 15)
    await api.assert_chain_complete("late-child", ["late-parent"])
    print_test_summary("Kafka out-of-order arrival", {
        "first_event": "late-child references missing late-parent",
        "first_result": "missing_parent warning",
        "second_event": "late-parent arrives later",
        "final_chain": ["late-parent"],
        "proof": "consumer accepts out-of-order events and chain completes after parent arrival",
    })


@pytest.mark.asyncio
async def test_poison_message_does_not_block_later_valid_event(kafka_test_producer, mongo_test_client, running_app_with_kafka):
    _, api = running_app_with_kafka
    await kafka_test_producer.send_raw("{not-json")
    await kafka_test_producer.send_event({"id": "after-poison", "title": "After poison"})
    await mongo_test_client.wait_for_task("after-poison", 15)
    await api.assert_task_exists("after-poison")
    print_test_summary("Kafka poison message", {
        "poison_message": "invalid JSON",
        "valid_event_after_poison": "after-poison",
        "proof": "poison message was skipped and did not block later valid persistence",
    })


@pytest.mark.asyncio
async def test_malformed_events_are_not_persisted(kafka_test_producer, mongo_test_client, running_app_with_kafka):
    malformed = [
        {"title": "Missing id"},
        {"id": "missing-title"},
        {"id": "", "title": "Blank id"},
        {"id": "blank-title", "title": ""},
        {"id": "bad-parent", "title": "Bad", "parent_task_id": 123},
        {"id": "bad-parent-empty", "title": "Bad", "parent_task_id": ""},
    ]
    for event in malformed:
        await kafka_test_producer.send_event(event)
    await kafka_test_producer.send_event({"id": "after-malformed", "title": "Valid"})
    await mongo_test_client.wait_for_task("after-malformed", 15)
    for event in malformed:
        if isinstance(event.get("id"), str) and event.get("id"):
            assert await mongo_test_client.get_task(event["id"]) is None
    print_test_summary("Kafka malformed events", {
        "malformed_events_sent": len(malformed),
        "valid_event_after_malformed": "after-malformed",
        "mongo_count": await mongo_test_client.count_tasks(),
        "proof": "malformed events were rejected and did not block valid event processing",
    })


@pytest.mark.asyncio
async def test_self_parent_event_persists_and_reads_safely(kafka_test_producer, mongo_test_client, running_app_with_kafka):
    _, api = running_app_with_kafka
    await kafka_test_producer.send_event({"id": "kafka-self", "title": "Self", "parent_task_id": "kafka-self"})
    await mongo_test_client.wait_for_task("kafka-self", 15)
    await api.assert_chain_incomplete("kafka-self", "self_parent")
    print_test_summary("Kafka self-parent", {
        "task_id": "kafka-self",
        "persisted": True,
        "warning": "self_parent",
        "proof": "bad graph data persists but dependency-chain read is safe",
    })


@pytest.mark.asyncio
async def test_circular_dependency_through_kafka_does_not_hang(kafka_test_producer, mongo_test_client, running_app_with_kafka):
    _, api = running_app_with_kafka
    await kafka_test_producer.send_events([
        {"id": "A", "title": "A", "parent_task_id": "B"},
        {"id": "B", "title": "B", "parent_task_id": "C"},
        {"id": "C", "title": "C", "parent_task_id": "B"},
    ])
    await mongo_test_client.wait_for_count(3, 15)
    await api.assert_chain_incomplete("A", "cycle_detected")
    print_test_summary("Kafka circular dependency", {
        "cycle_shape": "A -> B -> C -> B",
        "persisted_tasks": 3,
        "warning": "cycle_detected",
        "proof": "real Kafka-ingested cycle does not hang dependency-chain endpoint",
    })


@pytest.mark.asyncio
async def test_consumer_catches_up_events_produced_before_app_starts(kafka_test_producer, mongo_test_client, redis_test_client, test_settings):
    await mongo_test_client.clear_tasks()
    await redis_test_client.clear_test_keys()
    await kafka_test_producer.send_events([
        {"id": "catch-root", "title": "Root"},
        {"id": "catch-child", "title": "Child", "parent_task_id": "catch-root"},
    ])

    app = create_app(
        mongodb_url=test_settings.mongodb_url,
        mongodb_database=test_settings.mongodb_database,
        redis_url=test_settings.redis_url,
        redis_key_prefix=test_settings.redis_prefix,
        kafka_bootstrap_servers=test_settings.kafka_bootstrap_servers,
        kafka_topic=test_settings.kafka_topic,
        kafka_consumer_group=f"{test_settings.kafka_consumer_group}-catchup",
        enable_kafka_consumer=True,
    )
    await app.router.startup()
    from httpx import ASGITransport, AsyncClient
    try:
        client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
        api = TaskApiClient(client)
        await mongo_test_client.wait_for_count(2, 20)
        await api.assert_task_exists("catch-root")
        await api.assert_chain_complete("catch-child", ["catch-root"])
        print_test_summary("Kafka catch-up before consumer start", {
            "events_produced_before_consumer_start": ["catch-root", "catch-child"],
            "fresh_consumer_group": f"{test_settings.kafka_consumer_group}-catchup",
            "mongo_count": await mongo_test_client.count_tasks(),
            "proof": "consumer started later and consumed existing topic messages from earliest offset",
        })
    finally:
        await client.aclose()
        await app.router.shutdown()