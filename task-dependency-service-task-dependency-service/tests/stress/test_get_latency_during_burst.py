import asyncio
import os

import pytest

from tests.helpers.metrics import print_stress_summary


pytestmark = pytest.mark.stress



@pytest.mark.asyncio
async def test_get_latency_during_unrelated_kafka_burst(kafka_test_producer, mongo_test_client, redis_test_client, running_app_with_kafka, task_factory, test_prefix):
    _, api = running_app_with_kafka
    event_count = int(os.getenv("STRESS_EVENT_COUNT", "5000"))
    repeats = int(os.getenv("STRESS_GET_REPEATS", "100"))
    p95_limit = float(os.getenv("STRESS_GET_P95_MS", "500"))
    await redis_test_client.increment_data_version()
    stable, expected = task_factory.make_chain(100, f"{test_prefix}-stable")
    await mongo_test_client.insert_tasks(stable)
    await api.get_chain_with_headers(expected["final_task_id"])
    unrelated, _ = task_factory.make_mixed_burst(event_count, f"{test_prefix}-unrelated", 0.1, 0.01)
    burst_task = asyncio.create_task(kafka_test_producer.send_burst(unrelated))
    latency = await api.measure_chain_get(expected["final_task_id"], repeats)
    burst_metrics = await burst_task
    assert latency["error_count"] == 0
    assert latency["p95_ms"] < p95_limit
    await api.assert_chain_complete(expected["final_task_id"], expected["expected_chain_ids"])
    print_stress_summary("GET latency during burst", {**latency, **burst_metrics})