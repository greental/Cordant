import os

import pytest

from tests.helpers.metrics import print_stress_summary


pytestmark = pytest.mark.stress



@pytest.mark.asyncio
async def test_continuous_kafka_ingestion(kafka_test_producer, mongo_test_client, running_app_with_kafka, test_prefix):
    _, api = running_app_with_kafka
    duration = int(os.getenv("STRESS_DURATION_SECONDS", "30"))
    eps = int(os.getenv("STRESS_EVENTS_PER_SECOND", "100"))
    timeout = int(os.getenv("STRESS_TIMEOUT_SECONDS", "90"))

    def factory(index: int) -> dict:
        parent = f"{test_prefix}-continuous-{index - 1}" if index > 0 else None
        return {"id": f"{test_prefix}-continuous-{index}", "title": f"Continuous {index}", "parent_task_id": parent}

    metrics = await kafka_test_producer.send_continuous(factory, duration, eps)
    await mongo_test_client.wait_for_count(max(1, metrics["sent"]), timeout)
    sample_id = f"{test_prefix}-continuous-{max(0, metrics['sent'] - 1)}"
    response = await api.get_dependency_chain(sample_id)
    assert response.status_code != 500
    print_stress_summary("Continuous Kafka ingestion", {**metrics, "persisted_unique_count": await mongo_test_client.count_unique_ids()})