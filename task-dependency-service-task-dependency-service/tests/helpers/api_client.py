import time

from tests.helpers.metrics import summarize_latencies


class TaskApiClient:
    def __init__(self, client):
        self.client = client

    async def get_task(self, task_id: str):
        return await self.client.get(f"/tasks/{task_id}")

    async def get_dependency_chain(self, task_id: str):
        return await self.client.get(f"/tasks/{task_id}/dependency-chain")

    async def get_chain_with_headers(self, task_id: str) -> tuple[dict, dict]:
        response = await self.get_dependency_chain(task_id)
        response.raise_for_status()
        return response.json(), dict(response.headers)

    async def assert_task_exists(self, task_id: str):
        response = await self.get_task(task_id)
        assert response.status_code == 200
        assert response.json()["id"] == task_id
        return response.json()

    async def assert_task_missing(self, task_id: str):
        response = await self.get_task(task_id)
        assert response.status_code == 404

    async def assert_chain_complete(self, task_id: str, expected_ids: list[str]):
        response = await self.get_dependency_chain(task_id)
        assert response.status_code == 200
        body = response.json()
        assert body["complete"] is True
        assert [task["id"] for task in body["chain"]] == expected_ids
        return body

    async def assert_chain_incomplete(self, task_id: str, warning_code: str):
        response = await self.get_dependency_chain(task_id)
        assert response.status_code == 200
        body = response.json()
        assert body["complete"] is False
        assert any(warning["code"] == warning_code for warning in body["warnings"])
        return body

    async def measure_chain_get(self, task_id: str, repeats: int) -> dict:
        durations = []
        success_count = 0
        error_count = 0
        cache_counts = {"hit": 0, "miss": 0, "stale": 0, "bypass": 0}
        for _ in range(repeats):
            started = time.perf_counter()
            response = await self.get_dependency_chain(task_id)
            durations.append((time.perf_counter() - started) * 1000)
            if response.status_code < 500:
                success_count += 1
            else:
                error_count += 1
            cache_status = response.headers.get("X-Dependency-Chain-Cache", "").lower()
            if cache_status in cache_counts:
                cache_counts[cache_status] += 1
        summary = summarize_latencies(durations)
        return {
            "count": repeats,
            "success_count": success_count,
            "error_count": error_count,
            "p50_ms": summary["p50_ms"],
            "p95_ms": summary["p95_ms"],
            "max_ms": summary["max_ms"],
            "durations_ms": durations,
            "cache_hits": cache_counts["hit"],
            "cache_misses": cache_counts["miss"],
            "cache_stale": cache_counts["stale"],
            "cache_bypass": cache_counts["bypass"],
        }