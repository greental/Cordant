import pytest
from tests.helpers.metrics import print_test_summary


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_malformed_event_matrix_does_not_block_valid_events(kafka_test_producer, mongo_test_client, running_app_with_kafka, task_factory):
    for name, raw in task_factory.make_malformed_events():
        if name in {"unicode title valid case", "extremely long title", "extremely long id"}:
            continue
        if isinstance(raw, dict):
            await kafka_test_producer.send_event(raw)
        else:
            await kafka_test_producer.send_raw(raw)
    await kafka_test_producer.send_event({"id": "matrix-valid", "title": "Valid after matrix"})
    await mongo_test_client.wait_for_task("matrix-valid", 20)
    print_test_summary("Bad Kafka data matrix", {
        "malformed_cases_sent": len([case for case in task_factory.make_malformed_events() if case[0] not in {"unicode title valid case", "extremely long title", "extremely long id"}]),
        "valid_event_after_matrix": "matrix-valid",
        "mongo_count": await mongo_test_client.count_tasks(),
        "proof": "malformed/poison messages did not persist and did not block a later valid event",
    })
    assert await mongo_test_client.count_tasks() == 1


@pytest.mark.asyncio
async def test_valid_edge_data_and_graph_anomalies_are_safe(mongo_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await mongo_test_client.insert_tasks([
        {"id": "unicode", "title": "שלום 🚀", "extra": "ignored"},
        {"id": "missing-parent", "title": "Child", "parent_task_id": "nope"},
        {"id": "self", "title": "Self", "parent_task_id": "self"},
        {"id": "two-A", "title": "A", "parent_task_id": "two-B"},
        {"id": "two-B", "title": "B", "parent_task_id": "two-A"},
        {"id": "three-A", "title": "A", "parent_task_id": "three-B"},
        {"id": "three-B", "title": "B", "parent_task_id": "three-C"},
        {"id": "three-C", "title": "C", "parent_task_id": "three-A"},
        {"id": "indirect-A", "title": "A", "parent_task_id": "indirect-B"},
        {"id": "indirect-B", "title": "B", "parent_task_id": "indirect-C"},
        {"id": "indirect-C", "title": "C", "parent_task_id": "indirect-B"},
    ])
    assert (await api.assert_task_exists("unicode"))["title"] == "שלום 🚀"
    await api.assert_chain_incomplete("missing-parent", "missing_parent")
    await api.assert_chain_incomplete("self", "self_parent")
    await api.assert_chain_incomplete("two-A", "cycle_detected")
    await api.assert_chain_incomplete("three-A", "cycle_detected")
    body = await api.assert_chain_incomplete("indirect-A", "cycle_detected")
    assert [task["id"] for task in body["chain"]] == ["indirect-B", "indirect-C"]
    print_test_summary("Graph anomaly matrix", {
        "valid_unicode_title": "accepted",
        "missing_parent_warning": "missing_parent",
        "self_parent_warning": "self_parent",
        "two_node_cycle": "two-A -> two-B -> two-A",
        "three_node_cycle": "three-A -> three-B -> three-C -> three-A",
        "indirect_cycle_prefix": [task["id"] for task in body["chain"]],
        "proof": "all bad graph shapes returned safe warnings without hanging",
    })