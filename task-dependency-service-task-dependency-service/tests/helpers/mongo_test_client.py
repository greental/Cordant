from motor.motor_asyncio import AsyncIOMotorClient

from app.models import Task
from tests.helpers.polling import wait_until


class MongoTestClient:
    def __init__(self, mongodb_url: str, database_name: str, collection_name: str = "tasks"):
        self.mongodb_url = mongodb_url
        self.database_name = database_name
        self.collection_name = collection_name
        self.client: AsyncIOMotorClient | None = None
        self.collection = None

    async def start(self):
        self.client = AsyncIOMotorClient(self.mongodb_url)
        self.collection = self.client[self.database_name][self.collection_name]
        await self.collection.create_index("id", unique=True)
        await self.collection.create_index("parent_task_id")

    async def stop(self):
        if self.client is not None:
            self.client.close()

    async def clear_tasks(self):
        await self.collection.delete_many({})

    async def insert_task(self, task: dict):
        model = Task.model_validate(task)
        await self.collection.update_one({"id": model.id}, {"$set": model.model_dump()}, upsert=True)

    async def insert_tasks(self, tasks: list[dict]):
        for task in tasks:
            await self.insert_task(task)

    async def get_task(self, task_id: str) -> dict | None:
        doc = await self.collection.find_one({"id": task_id}, {"_id": 0})
        return doc

    async def count_by_id(self, task_id: str) -> int:
        return await self.collection.count_documents({"id": task_id})

    async def count_tasks(self) -> int:
        return await self.collection.count_documents({})

    async def count_unique_ids(self) -> int:
        return len(await self.collection.distinct("id"))

    async def wait_for_task(self, task_id: str, timeout_seconds: float):
        async def predicate() -> bool:
            return await self.get_task(task_id) is not None

        await wait_until(predicate, timeout_seconds, description=f"Mongo task {task_id}")
        return await self.get_task(task_id)

    async def wait_for_count(self, expected_count: int, timeout_seconds: float):
        async def predicate() -> bool:
            return await self.count_tasks() >= expected_count

        await wait_until(predicate, timeout_seconds, description=f"Mongo count >= {expected_count}")

    async def list_indexes(self) -> list[dict]:
        return await self.collection.list_indexes().to_list(length=None)

    async def assert_unique_id_index(self):
        indexes = await self.list_indexes()
        assert any(index.get("key") == {"id": 1} and index.get("unique") for index in indexes)

    async def assert_parent_task_id_index(self):
        indexes = await self.list_indexes()
        assert any(index.get("key") == {"parent_task_id": 1} for index in indexes)