import redis.asyncio as redis
from motor.motor_asyncio import AsyncIOMotorClient

from app.config import settings

mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(settings.mongodb_url)
db = mongo_client[settings.mongodb_database]
tasks_collection = db["tasks"]

redis_client: redis.Redis = redis.from_url(settings.redis_url, decode_responses=True)
