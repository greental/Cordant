import os

import pytest

from tests.helpers.metrics import print_stress_summary


pytestmark = pytest.mark.stress


@pytest.mark.asyncio
async def test_kafka_burst_ingestion(kafka_test_producer, mongo_test_client, running_app_with_kafka, task_factory, test_prefix):
    event_count = int(os.getenv("STRESS_EVENT_COUNT", "5000"))
    timeout = int(os.getenv("STRESS_TIMEOUT_SECONDS", "60"))
    duplicates_rate = float(os.getenv("STRESS_DUPLICATES_RATE", "0.1"))
    malformed_rate = float(os.getenv("STRESS_MALFORMED_RATE", "0.01"))
    events, expected = task_factory.make_mixed_burst(event_count, f"{test_prefix}-burst", duplicates_rate, malformed_rate)
    metrics = await kafka_test_producer.send_burst(events)
    await mongo_test_client.wait_for_count(expected["expected_valid_unique_count"], timeout)
    assert await mongo_test_client.count_unique_ids() == expected["expected_valid_unique_count"]
    print_stress_summary("Kafka burst ingestion", {**metrics, **expected, "mongo_count": await mongo_test_client.count_tasks()})