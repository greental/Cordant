import pytest
from tests.helpers.metrics import Timer, print_test_summary


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_cache_improves_repeated_reads_by_source_and_lookup_count(mongo_test_client, redis_test_client, running_app_without_kafka, task_factory, test_prefix):
    _, api = running_app_without_kafka
    await redis_test_client.increment_data_version()
    tasks, expected = task_factory.make_chain(length=500, prefix=f"{test_prefix}-perf")
    await mongo_test_client.insert_tasks(tasks)
    with Timer() as first_timer:
        first, first_headers = await api.get_chain_with_headers(expected["final_task_id"])
    with Timer() as second_timer:
        second, second_headers = await api.get_chain_with_headers(expected["final_task_id"])
    print_test_summary("Cache performance proof", {
        "mongo_seeded_tasks": len(tasks),
        "final_task": expected["final_task_id"],
        "first_get_ms_without_cache_hit": round(first_timer.duration_ms, 3),
        "first_cache_status": first_headers["x-dependency-chain-cache"],
        "first_source": first_headers["x-dependency-chain-source"],
        "first_mongo_lookups": first_headers["x-dependency-chain-mongo-lookups"],
        "second_get_ms_using_cache": round(second_timer.duration_ms, 3),
        "second_cache_status": second_headers["x-dependency-chain-cache"],
        "second_source": second_headers["x-dependency-chain-source"],
        "second_mongo_lookups": second_headers["x-dependency-chain-mongo-lookups"],
        "proof": "same body; second GET was served from Redis with zero Mongo lookups",
    })
    assert second == first
    assert first_headers["x-dependency-chain-cache"] == "miss"
    assert first_headers["x-dependency-chain-source"] == "mongo"
    assert second_headers["x-dependency-chain-cache"] == "hit"
    assert second_headers["x-dependency-chain-source"] == "redis"
    assert int(second_headers["x-dependency-chain-mongo-lookups"]) == 0