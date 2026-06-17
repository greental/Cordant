import pytest
from tests.helpers.metrics import print_test_summary


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_cache_disabled_bypasses_redis(test_settings, mongo_test_client, redis_test_client, clean_datastores):
    from tests.conftest import _make_api_client

    tasks = [
        {"id": "cache-root", "title": "Root"},
        {"id": "cache-child", "title": "Child", "parent_task_id": "cache-root"},
    ]
    await mongo_test_client.insert_tasks(tasks)
    app, client = await _make_api_client(test_settings, enable_kafka_consumer=False, cache_enabled=False)
    try:
        from tests.helpers.api_client import TaskApiClient
        api = TaskApiClient(client)
        body, headers = await api.get_chain_with_headers("cache-child")
        print_test_summary("Redis cache disabled", {
            "seeded_tasks": len(tasks),
            "chain_complete": body["complete"],
            "cache_header": headers["x-dependency-chain-cache"],
            "proof": "cache disabled returns correct Mongo-computed chain and writes no Redis chain key",
        })
        assert body["complete"] is True
        assert headers["x-dependency-chain-cache"] == "bypass"
        await redis_test_client.assert_no_dependency_chain_cache("cache-child")
    finally:
        await client.aclose()
        await app.router.shutdown()


@pytest.mark.asyncio
async def test_cache_miss_then_hit_writes_ttl_payload(mongo_test_client, redis_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await redis_test_client.increment_data_version()
    await mongo_test_client.insert_tasks([
        {"id": "root", "title": "Root"},
        {"id": "child", "title": "Child", "parent_task_id": "root"},
    ])
    first, first_headers = await api.get_chain_with_headers("child")
    assert first_headers["x-dependency-chain-cache"] == "miss"
    assert first_headers["x-dependency-chain-source"] == "mongo"
    await redis_test_client.assert_dependency_chain_cache_exists("child")
    cached = await redis_test_client.get_dependency_chain_cache("child")
    assert "data_version" in cached
    assert await redis_test_client.ttl("task:child:dependency-chain") > 0

    second, second_headers = await api.get_chain_with_headers("child")
    print_test_summary("Redis miss then hit", {
        "seeded_tasks": 2,
        "first_cache_status": first_headers["x-dependency-chain-cache"],
        "first_source": first_headers["x-dependency-chain-source"],
        "redis_payload_has_data_version": "data_version" in cached,
        "cache_ttl_seconds": await redis_test_client.ttl("task:child:dependency-chain"),
        "second_cache_status": second_headers["x-dependency-chain-cache"],
        "second_source": second_headers["x-dependency-chain-source"],
        "second_mongo_lookups": second_headers["x-dependency-chain-mongo-lookups"],
        "proof": "second identical GET was served from Redis with zero Mongo lookups",
    })
    assert second == first
    assert second_headers["x-dependency-chain-cache"] == "hit"
    assert second_headers["x-dependency-chain-source"] == "redis"
    assert int(second_headers["x-dependency-chain-mongo-lookups"]) == 0


@pytest.mark.asyncio
async def test_stale_cache_recomputes_after_data_version_increment(mongo_test_client, redis_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await redis_test_client.increment_data_version()
    await mongo_test_client.insert_tasks([
        {"id": "root", "title": "Root"},
        {"id": "child", "title": "Child", "parent_task_id": "root"},
    ])
    await api.get_chain_with_headers("child")
    await mongo_test_client.insert_task({"id": "root", "title": "Root updated"})
    await redis_test_client.increment_data_version()
    body, headers = await api.get_chain_with_headers("child")
    print_test_summary("Redis stale cache", {
        "updated_task": "root",
        "cache_status_after_version_increment": headers["x-dependency-chain-cache"],
        "returned_parent_title": body["chain"][0]["title"],
        "proof": "data version increment made the old cached payload stale and forced Mongo recompute",
    })
    assert headers["x-dependency-chain-cache"] == "stale"
    assert body["chain"][0]["title"] == "Root updated"


@pytest.mark.asyncio
async def test_incomplete_response_is_not_cached_then_complete_can_be_cached(mongo_test_client, redis_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await redis_test_client.increment_data_version()
    await mongo_test_client.insert_task({"id": "child", "title": "Child", "parent_task_id": "missing"})
    await api.assert_chain_incomplete("child", "missing_parent")
    await redis_test_client.assert_no_dependency_chain_cache("child")
    await mongo_test_client.insert_task({"id": "missing", "title": "Parent"})
    await redis_test_client.increment_data_version()
    await api.assert_chain_complete("child", ["missing"])
    await redis_test_client.assert_dependency_chain_cache_exists("child")
    _, headers = await api.get_chain_with_headers("child")
    print_test_summary("Incomplete chain cache policy", {
        "initial_warning": "missing_parent",
        "cached_while_incomplete": False,
        "after_parent_arrival_cache_status": headers["x-dependency-chain-cache"],
        "proof": "incomplete chains are not cached; complete chain is cached after parent exists",
    })
    assert headers["x-dependency-chain-cache"] == "hit"


@pytest.mark.asyncio
async def test_ttl_is_set_and_cache_does_not_live_forever(mongo_test_client, redis_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await redis_test_client.increment_data_version()
    await mongo_test_client.insert_task({"id": "ttl-root", "title": "Root"})
    await api.get_chain_with_headers("ttl-root")
    ttl = await redis_test_client.ttl("task:ttl-root:dependency-chain")
    print_test_summary("Redis cache TTL", {
        "task": "ttl-root",
        "ttl_seconds": ttl,
        "proof": "cache key has a finite TTL and does not live forever",
    })
    assert ttl > 0
    assert ttl <= 60