import pytest
from tests.helpers.metrics import print_test_summary


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_get_task_response_shape(mongo_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await mongo_test_client.insert_task({"id": "api-task", "title": "API task"})
    response = await api.get_task("api-task")
    assert response.status_code == 200
    assert response.json() == {"id": "api-task", "title": "API task", "parent_task_id": None}


@pytest.mark.asyncio
async def test_get_missing_task_returns_404(running_app_without_kafka):
    _, api = running_app_without_kafka
    await api.assert_task_missing("api-missing")


@pytest.mark.asyncio
async def test_dependency_chain_for_root(mongo_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await mongo_test_client.insert_task({"id": "root", "title": "Root"})
    body = await api.assert_chain_complete("root", [])
    assert body["warnings"] == []


@pytest.mark.asyncio
async def test_dependency_chain_for_full_chain(mongo_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await mongo_test_client.insert_tasks([
        {"id": "root", "title": "Root"},
        {"id": "child", "title": "Child", "parent_task_id": "root"},
        {"id": "grandchild", "title": "Grandchild", "parent_task_id": "child"},
    ])
    await api.assert_chain_complete("grandchild", ["child", "root"])
    print_test_summary("API full dependency chain", {
        "shape": "grandchild -> child -> root",
        "expected_order": ["child", "root"],
        "proof": "chain order is immediate parent first through root",
    })


@pytest.mark.asyncio
async def test_missing_requested_task_returns_404(running_app_without_kafka):
    _, api = running_app_without_kafka
    response = await api.get_dependency_chain("missing")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_missing_parent_returns_partial_warning(mongo_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await mongo_test_client.insert_task({"id": "child", "title": "Child", "parent_task_id": "missing"})
    await api.assert_chain_incomplete("child", "missing_parent")
    print_test_summary("API missing parent", {
        "task": "child",
        "missing_parent": "missing",
        "warning": "missing_parent",
        "proof": "endpoint returns partial incomplete chain instead of hanging or 500",
    })


@pytest.mark.asyncio
async def test_self_parent_returns_warning(mongo_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await mongo_test_client.insert_task({"id": "self", "title": "Self", "parent_task_id": "self"})
    await api.assert_chain_incomplete("self", "self_parent")
    print_test_summary("API self-parent", {
        "shape": "self -> self",
        "warning": "self_parent",
        "proof": "self reference is detected and excluded from its own chain",
    })


@pytest.mark.asyncio
async def test_circular_dependency_returns_warning(mongo_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await mongo_test_client.insert_tasks([
        {"id": "A", "title": "A", "parent_task_id": "B"},
        {"id": "B", "title": "B", "parent_task_id": "C"},
        {"id": "C", "title": "C", "parent_task_id": "B"},
    ])
    body = await api.assert_chain_incomplete("A", "cycle_detected")
    assert [task["id"] for task in body["chain"]] == ["B", "C"]
    print_test_summary("API circular dependency", {
        "shape": "A -> B -> C -> B",
        "acyclic_prefix": [task["id"] for task in body["chain"]],
        "warning": "cycle_detected",
        "proof": "cycle returns a safe acyclic prefix and does not hang",
    })


@pytest.mark.asyncio
async def test_completed_same_chain_after_parent_arrival(mongo_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await mongo_test_client.insert_task({"id": "late-child", "title": "Child", "parent_task_id": "late-parent"})
    await api.assert_chain_incomplete("late-child", "missing_parent")
    await mongo_test_client.insert_task({"id": "late-parent", "title": "Parent"})
    await api.assert_chain_complete("late-child", ["late-parent"])
    print_test_summary("API completed chain after parent arrival", {
        "initial_state": "late-child referenced missing late-parent",
        "initial_warning": "missing_parent",
        "after_insert": "late-parent added",
        "final_chain": ["late-parent"],
        "proof": "same child chain becomes complete once parent data exists",
    })