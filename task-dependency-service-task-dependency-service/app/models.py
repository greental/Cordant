import uuid
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class Task(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    title: str
    parent_task_id: Optional[str] = None


class DependencyWarning(BaseModel):
    code: str
    message: str


class DependencyChainResponse(BaseModel):
    task_id: str
    chain: list[Task]
    complete: bool
    warnings: list[DependencyWarning] = Field(default_factory=list)


class DependencyChainResult(BaseModel):
    response: DependencyChainResponse
    cache_status: str = "bypass"
    source: str = "mongo"
    data_version: int | None = None
    mongo_lookup_count: int | None = None


class CachedDependencyChainResponse(BaseModel):
    data_version: int
    response: DependencyChainResponse
