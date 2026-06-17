import pytest

from app.models import Task
from tests.helpers.metrics import print_test_summary


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_indexes_are_created(mongo_test_client, clean_datastores):
    await mongo_test_client.assert_unique_id_index()
    await mongo_test_client.assert_parent_task_id_index()
    print_test_summary("Mongo indexes", {
        "unique_id_index": True,
        "parent_task_id_index": True,
        "proof": "repository supports idempotent upsert and parent lookup access pattern",
    })


@pytest.mark.asyncio
async def test_upsert_inserts_new_task(mongo_test_client, clean_datastores):
    await mongo_test_client.insert_task({"id": "mongo-new", "title": "New"})
    assert await mongo_test_client.get_task("mongo-new") == {"id": "mongo-new", "title": "New", "parent_task_id": None}


@pytest.mark.asyncio
async def test_duplicate_upsert_leaves_one_document(mongo_test_client, clean_datastores):
    task = {"id": "mongo-dupe", "title": "Dupe"}
    await mongo_test_client.insert_task(task)
    await mongo_test_client.insert_task(task)
    assert await mongo_test_client.count_by_id("mongo-dupe") == 1
    print_test_summary("Mongo duplicate upsert", {
        "task_id": "mongo-dupe",
        "upsert_attempts": 2,
        "documents_for_id": await mongo_test_client.count_by_id("mongo-dupe"),
        "proof": "duplicate replay leaves exactly one document",
    })


@pytest.mark.asyncio
async def test_same_id_update_changes_title(mongo_test_client, clean_datastores):
    await mongo_test_client.insert_task({"id": "mongo-update", "title": "Old"})
    await mongo_test_client.insert_task({"id": "mongo-update", "title": "New"})
    assert (await mongo_test_client.get_task("mongo-update"))["title"] == "New"
    print_test_summary("Mongo same-id title update", {
        "task_id": "mongo-update",
        "final_title": "New",
        "proof": "upsert updates existing task fields",
    })


@pytest.mark.asyncio
async def test_same_id_update_changes_parent_task_id(mongo_test_client, clean_datastores):
    await mongo_test_client.insert_task({"id": "mongo-parent", "title": "Task", "parent_task_id": "old"})
    await mongo_test_client.insert_task({"id": "mongo-parent", "title": "Task", "parent_task_id": "new"})
    assert (await mongo_test_client.get_task("mongo-parent"))["parent_task_id"] == "new"


@pytest.mark.asyncio
async def test_get_task_returns_existing_task(mongo_test_client, clean_datastores):
    await mongo_test_client.insert_task({"id": "mongo-existing", "title": "Existing"})
    assert (await mongo_test_client.get_task("mongo-existing"))["id"] == "mongo-existing"


@pytest.mark.asyncio
async def test_get_task_returns_none_for_missing(mongo_test_client, clean_datastores):
    assert await mongo_test_client.get_task("missing") is None


@pytest.mark.asyncio
async def test_api_get_task_reads_task_persisted_in_mongo(mongo_test_client, running_app_without_kafka):
    _, api = running_app_without_kafka
    await mongo_test_client.insert_task({"id": "mongo-api", "title": "API"})
    body = await api.assert_task_exists("mongo-api")
    assert body == {"id": "mongo-api", "title": "API", "parent_task_id": None}


@pytest.mark.asyncio
async def test_duplicate_replay_does_not_corrupt_document(mongo_test_client, clean_datastores):
    await mongo_test_client.insert_task({"id": "mongo-replay", "title": "One"})
    await mongo_test_client.insert_task({"id": "mongo-replay", "title": "One"})
    assert await mongo_test_client.count_by_id("mongo-replay") == 1
    assert await mongo_test_client.count_unique_ids() == 1