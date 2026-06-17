import os

import pytest

from tests.helpers.metrics import print_stress_summary


pytestmark = pytest.mark.stress



@pytest.mark.asyncio
async def test_long_chain_within_limit_and_cache_hit(mongo_test_client, redis_test_client, running_app_without_kafka, task_factory, test_prefix, test_settings):
    _, api = running_app_without_kafka
    length = int(os.getenv("STRESS_CHAIN_LENGTH", "1000"))
    length = min(length, test_settings.max_dependency_depth)
    await redis_test_client.increment_data_version()
    tasks, expected = task_factory.make_chain(length, f"{test_prefix}-long")
    await mongo_test_client.insert_tasks(tasks)
    body = await api.assert_chain_complete(expected["final_task_id"], expected["expected_chain_ids"])
    _, headers = await api.get_chain_with_headers(expected["final_task_id"])
    assert headers["x-dependency-chain-cache"] == "hit"
    print_stress_summary("Long chain", {"length": length, "chain_length": len(body["chain"])})


@pytest.mark.asyncio
async def test_chain_larger_than_max_depth_returns_warning(mongo_test_client, running_app_without_kafka, task_factory, test_prefix, test_settings):
    _, api = running_app_without_kafka
    length = test_settings.max_dependency_depth + 5
    tasks, expected = task_factory.make_chain(length, f"{test_prefix}-over")
    await mongo_test_client.insert_tasks(tasks)
    await api.assert_chain_incomplete(expected["final_task_id"], "max_depth_exceeded")
    print_stress_summary("Over-limit chain", {
        "length": length,
        "max_dependency_depth": test_settings.max_dependency_depth,
        "warning": "max_depth_exceeded",
        "proof": "resolver stops at max depth instead of traversing indefinitely",
    })


@pytest.mark.asyncio
async def test_large_circular_chain_does_not_hang(mongo_test_client, running_app_without_kafka, task_factory, test_prefix):
    _, api = running_app_without_kafka
    tasks, expected = task_factory.make_chain(200, f"{test_prefix}-cycle")
    tasks[0]["parent_task_id"] = tasks[-1]["id"]
    await mongo_test_client.insert_tasks(tasks)
    await api.assert_chain_incomplete(expected["final_task_id"], "cycle_detected")
    print_stress_summary("Large circular chain", {
        "length": len(tasks),
        "cycle_edge": f"{tasks[0]['id']} -> {tasks[-1]['id']}",
        "warning": "cycle_detected",
        "proof": "large cycle returns safely without timeout/hang",
    })