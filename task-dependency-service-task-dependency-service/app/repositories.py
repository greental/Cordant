from motor.motor_asyncio import AsyncIOMotorCollection

from app.db import tasks_collection
from app.models import Task


class TaskRepository:
    def __init__(self, collection: AsyncIOMotorCollection = tasks_collection):
        self.collection = collection

    async def ensure_indexes(self) -> None:
        await self.collection.create_index("id", unique=True)
        await self.collection.create_index("parent_task_id")

    async def get_task(self, task_id: str) -> Task | None:
        doc = await self.collection.find_one({"id": task_id})
        if not doc:
            return None
        return Task(**doc)

    async def upsert_task(self, task: Task) -> Task:
        task_data = task.model_dump()
        await self.collection.update_one(
            {"id": task.id},
            {"$set": task_data},
            upsert=True,
        )
        return task


task_repository = TaskRepository()