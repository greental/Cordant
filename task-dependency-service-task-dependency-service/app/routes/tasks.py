from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from app.models import DependencyChainResponse, Task
from app.repositories import TaskRepository
from app.services import DependencyChainService, TaskNotFoundError

router = APIRouter(prefix="/tasks", tags=["tasks"])


def get_task_repository(request: Request) -> TaskRepository:
    return request.app.state.task_repository


def get_dependency_chain_service(request: Request) -> DependencyChainService:
    return request.app.state.dependency_chain_service


@router.get("/{task_id}", response_model=Task)
async def get_task(
    task_id: str,
    repository: Annotated[TaskRepository, Depends(get_task_repository)],
):
    task = await repository.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.get("/{task_id}/dependency-chain", response_model=DependencyChainResponse)
async def get_dependency_chain(
    task_id: str,
    response: Response,
    service: Annotated[DependencyChainService, Depends(get_dependency_chain_service)],
):
    try:
        result = await service.get_dependency_chain_result(task_id)
    except TaskNotFoundError:
        raise HTTPException(status_code=404, detail="Task not found") from None

    response.headers["X-Dependency-Chain-Cache"] = result.cache_status
    response.headers["X-Dependency-Chain-Source"] = result.source
    if result.data_version is not None:
        response.headers["X-Dependency-Chain-Data-Version"] = str(result.data_version)
    if result.mongo_lookup_count is not None:
        response.headers["X-Dependency-Chain-Mongo-Lookups"] = str(result.mongo_lookup_count)
    return result.response
