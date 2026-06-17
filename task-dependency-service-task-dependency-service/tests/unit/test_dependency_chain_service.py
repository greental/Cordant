import pytest

from app.models import DependencyChainResponse, Task
from app.services import DependencyChainService, TaskNotFoundError
from tests.helpers.task_factory import TaskFactory


pytestmark = pytest.mark.unit


class NoopCache:
    enabled = False

    async def get_chain(self, task_id: str):
        return None

    async def set_chain(self, response: DependencyChainResponse) -> None:
        return None

    async def get_data_version(self):
        return None


class FakeResultCache:
    def __init__(self, *, enabled=True, version=7, cached_response=None, version_available=True):
        self.enabled = enabled
        self.version = version
        self.cached_response = cached_response
        self.version_available = version_available
        self.set_count = 0

    async def get_data_version(self):
        return self.version if self.version_available else None

    async def get_chain(self, task_id: str):
        return self.cached_response

    async def get_chain_lookup(self, task_id: str, current_version: int | None = None):
        from app.cache import DependencyChainCacheLookup

        if self.cached_response is not None:
            return DependencyChainCacheLookup(status="hit", response=self.cached_response)
        return DependencyChainCacheLookup(status="miss")

    async def set_chain(self, response: DependencyChainResponse) -> None:
        self.set_count += 1


class MemoryTaskRepository:
    def __init__(self):
        self.tasks: dict[str, Task] = {}

    async def get_task(self, task_id: str) -> Task | None:
        return self.tasks.get(task_id)

    async def upsert_task(self, task: Task) -> Task:
        self.tasks[task.id] = task
        return task


async def service_with(
    tasks: list[Task],
    max_depth: int = 1000,
    depth_limit_enabled: bool = True,
):
    repository = MemoryTaskRepository()
    for task in tasks:
        await repository.upsert_task(task)
    return DependencyChainService(
        repository=repository,
        cache=NoopCache(),
        max_depth=max_depth,
        depth_limit_enabled=depth_limit_enabled,
    )


async def repository_with(tasks: list[Task]) -> MemoryTaskRepository:
    repository = MemoryTaskRepository()
    for task in tasks:
        await repository.upsert_task(task)
    return repository


@pytest.mark.asyncio
async def test_root_task_returns_empty_complete_chain():
    service = await service_with([Task(id="root", title="Root")])
    response = await service.get_dependency_chain("root")
    assert response.chain == []
    assert response.complete is True
    assert response.warnings == []


@pytest.mark.asyncio
async def test_one_level_chain_returns_parent():
    service = await service_with([
        Task(id="root", title="Root"),
        Task(id="child", title="Child", parent_task_id="root"),
    ])
    response = await service.get_dependency_chain("child")
    assert [task.id for task in response.chain] == ["root"]


@pytest.mark.asyncio
async def test_multi_level_chain_order_is_immediate_parent_first():
    service = await service_with([
        Task(id="root", title="Root"),
        Task(id="child", title="Child", parent_task_id="root"),
        Task(id="grandchild", title="Grandchild", parent_task_id="child"),
    ])
    response = await service.get_dependency_chain("grandchild")
    assert [task.id for task in response.chain] == ["child", "root"]
    assert response.complete is True


@pytest.mark.asyncio
async def test_missing_requested_task_raises_not_found():
    service = await service_with([])
    with pytest.raises(TaskNotFoundError):
        await service.get_dependency_chain("missing")


@pytest.mark.asyncio
async def test_missing_parent_returns_partial_warning():
    service = await service_with([Task(id="child", title="Child", parent_task_id="missing-parent")])
    response = await service.get_dependency_chain("child")
    assert response.chain == []
    assert response.complete is False
    assert response.warnings[0].code == "missing_parent"


@pytest.mark.asyncio
async def test_self_parent_returns_self_parent_warning_without_self_in_chain():
    service = await service_with([Task(id="A", title="A", parent_task_id="A")])
    response = await service.get_dependency_chain("A")
    assert response.chain == []
    assert response.complete is False
    assert response.warnings[0].code == "self_parent"


@pytest.mark.asyncio
async def test_circular_dependency_returns_acyclic_prefix():
    service = await service_with([
        Task(id="A", title="A", parent_task_id="B"),
        Task(id="B", title="B", parent_task_id="C"),
        Task(id="C", title="C", parent_task_id="B"),
    ])
    response = await service.get_dependency_chain("A")
    assert [task.id for task in response.chain] == ["B", "C"]
    assert response.complete is False
    assert response.warnings[0].code == "cycle_detected"


@pytest.mark.asyncio
async def test_max_depth_stops_safely():
    service = await service_with([
        Task(id="root", title="Root"),
        Task(id="parent", title="Parent", parent_task_id="root"),
        Task(id="child", title="Child", parent_task_id="parent"),
    ], max_depth=1)
    response = await service.get_dependency_chain("child")
    assert [task.id for task in response.chain] == ["parent"]
    assert response.complete is False
    assert response.warnings[0].code == "max_depth_exceeded"


@pytest.mark.asyncio
async def test_depth_limit_can_be_disabled():
    service = await service_with([
        Task(id="root", title="Root"),
        Task(id="parent", title="Parent", parent_task_id="root"),
        Task(id="child", title="Child", parent_task_id="parent"),
    ], max_depth=1, depth_limit_enabled=False)
    response = await service.get_dependency_chain("child")
    assert [task.id for task in response.chain] == ["parent", "root"]
    assert response.complete is True
    assert response.warnings == []


@pytest.mark.asyncio
async def test_long_valid_chain_under_max_depth(task_factory):
    tasks, expected = TaskFactory.make_chain(length=100, prefix="unit-long")
    service = await service_with([Task.model_validate(task) for task in tasks], max_depth=200)
    response = await service.get_dependency_chain(expected["final_task_id"])
    assert [task.id for task in response.chain] == expected["expected_chain_ids"]
    assert response.complete is True


@pytest.mark.asyncio
async def test_get_dependency_chain_result_cache_disabled_bypasses_cache():
    repository = await repository_with([Task(id="root", title="Root")])
    service = DependencyChainService(repository=repository, cache=FakeResultCache(enabled=False))
    result = await service.get_dependency_chain_result("root")
    assert result.response.complete is True
    assert result.cache_status == "bypass"
    assert result.source == "mongo"
    assert result.mongo_lookup_count == 1


@pytest.mark.asyncio
async def test_get_dependency_chain_result_cache_hit_reports_redis_and_zero_mongo_lookups():
    cached = DependencyChainResponse(task_id="root", chain=[], complete=True, warnings=[])
    repository = await repository_with([])
    service = DependencyChainService(repository=repository, cache=FakeResultCache(cached_response=cached, version=3))
    result = await service.get_dependency_chain_result("root")
    assert result.response == cached
    assert result.cache_status == "hit"
    assert result.source == "redis"
    assert result.data_version == 3
    assert result.mongo_lookup_count == 0


@pytest.mark.asyncio
async def test_get_dependency_chain_result_cache_miss_computes_and_writes_cache():
    cache = FakeResultCache(cached_response=None, version=4)
    repository = await repository_with([Task(id="root", title="Root")])
    service = DependencyChainService(repository=repository, cache=cache)
    result = await service.get_dependency_chain_result("root")
    assert result.cache_status == "miss"
    assert result.source == "mongo"
    assert result.data_version == 4
    assert result.mongo_lookup_count == 1
    assert cache.set_count == 1


@pytest.mark.asyncio
async def test_get_dependency_chain_result_version_unavailable_bypasses_cache():
    cache = FakeResultCache(version_available=False)
    repository = await repository_with([Task(id="root", title="Root")])
    service = DependencyChainService(repository=repository, cache=cache)
    result = await service.get_dependency_chain_result("root")
    assert result.cache_status == "bypass"
    assert result.source == "mongo"
    assert result.data_version is None
    assert cache.set_count == 0


@pytest.mark.asyncio
async def test_get_dependency_chain_result_stale_cache_reports_stale_and_recomputes():
    from app.cache import DependencyChainCacheLookup

    class StaleCache(FakeResultCache):
        async def get_chain_lookup(self, task_id: str, current_version: int | None = None):
            return DependencyChainCacheLookup(status="stale")

    cache = StaleCache(version=5)
    repository = await repository_with([Task(id="root", title="Root")])
    service = DependencyChainService(repository=repository, cache=cache)
    result = await service.get_dependency_chain_result("root")
    assert result.cache_status == "stale"
    assert result.source == "mongo"
    assert result.data_version == 5
    assert cache.set_count == 1