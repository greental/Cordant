from app.cache import DependencyChainCache, dependency_chain_cache
from app.config import settings
from app.models import DependencyChainResponse, DependencyChainResult, DependencyWarning, Task
from app.repositories import TaskRepository, task_repository


class TaskNotFoundError(Exception):
    pass


class DependencyChainService:
    def __init__(
        self,
        repository: TaskRepository = task_repository,
        cache: DependencyChainCache = dependency_chain_cache,
        max_depth: int = settings.max_dependency_depth or settings.dependency_chain_max_depth,
    ):
        self.repository = repository
        self.cache = cache
        self.max_depth = max_depth

    async def get_dependency_chain(self, task_id: str) -> DependencyChainResponse:
        result = await self.get_dependency_chain_result(task_id)
        return result.response

    async def get_dependency_chain_result(self, task_id: str) -> DependencyChainResult:
        cache_enabled = getattr(self.cache, "enabled", True)
        if not cache_enabled:
            response, lookup_count = await self._compute_dependency_chain(task_id)
            return self._result_from_mongo(response, "bypass", None, lookup_count)

        current_version = await self.cache.get_data_version()
        if current_version is None:
            response, lookup_count = await self._compute_dependency_chain(task_id)
            return self._result_from_mongo(response, "bypass", None, lookup_count)

        cache_lookup = await self.cache.get_chain_lookup(task_id, current_version)
        if cache_lookup.response is not None:
            return DependencyChainResult(
                response=cache_lookup.response,
                cache_status="hit",
                source="redis",
                data_version=current_version,
                mongo_lookup_count=0,
            )

        response, lookup_count = await self._compute_dependency_chain(task_id)
        await self.cache.set_chain(response)
        cache_status = "stale" if cache_lookup.status == "stale" else "miss"
        return self._result_from_mongo(
            response,
            cache_status,
            current_version,
            lookup_count,
        )

    @staticmethod
    def _result_from_mongo(
        response: DependencyChainResponse,
        cache_status: str,
        data_version: int | None,
        lookup_count: int,
    ) -> DependencyChainResult:
        return DependencyChainResult(
            response=response,
            cache_status=cache_status,
            source="mongo",
            data_version=data_version,
            mongo_lookup_count=lookup_count,
        )

    async def _compute_dependency_chain(self, task_id: str) -> tuple[DependencyChainResponse, int]:
        lookup_count = 0
        task = await self.repository.get_task(task_id)
        lookup_count += 1
        if task is None:
            raise TaskNotFoundError(task_id)

        chain: list[Task] = []
        warnings: list[DependencyWarning] = []
        visited = {task.id}
        current_parent_id = task.parent_task_id

        while current_parent_id is not None:
            if current_parent_id == task.id and not chain:
                warnings.append(
                    DependencyWarning(
                        code="self_parent",
                        message=f"Task '{task_id}' references itself as parent",
                    )
                )
                break

            if current_parent_id in visited:
                warnings.append(
                    DependencyWarning(
                        code="cycle_detected",
                        message=f"Circular dependency detected at task '{current_parent_id}'",
                    )
                )
                break

            if len(chain) >= self.max_depth:
                warnings.append(
                    DependencyWarning(
                        code="max_depth_exceeded",
                        message=f"Dependency chain exceeded max depth of {self.max_depth}",
                    )
                )
                break

            visited.add(current_parent_id)
            parent = await self.repository.get_task(current_parent_id)
            lookup_count += 1
            if parent is None:
                warnings.append(
                    DependencyWarning(
                        code="missing_parent",
                        message=f"Parent task '{current_parent_id}' was referenced but not found",
                    )
                )
                break

            chain.append(parent)
            current_parent_id = parent.parent_task_id

        response = DependencyChainResponse(
            task_id=task_id,
            chain=chain,
            complete=len(warnings) == 0,
            warnings=warnings,
        )
        return response, lookup_count


dependency_chain_service = DependencyChainService()