from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env")

    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_database: str = "taskdb"
    test_mongodb_database: str = "taskdb_test"
    redis_url: str = "redis://localhost:6379"
    redis_key_prefix: str = ""
    test_redis_prefix: str = ""
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "tasks.events"
    kafka_consumer_group: str = "task-dependency-service"
    enable_kafka_consumer: bool = True
    dependency_chain_cache_enabled: bool = True
    dependency_chain_max_depth: int = 1000
    max_dependency_depth: int | None = None
    dependency_chain_cache_ttl_seconds: int = 60


settings = Settings()
