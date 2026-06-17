import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any


async def wait_until(
    predicate: Callable[[], Awaitable[bool] | bool],
    timeout_seconds: float,
    interval_seconds: float = 0.1,
    description: str = "",
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if asyncio.iscoroutine(result):
                result = await result
            if result:
                return
        except Exception as exc:
            last_error = exc
        await asyncio.sleep(interval_seconds)

    message = f"Timed out after {timeout_seconds}s"
    if description:
        message += f" waiting for {description}"
    if last_error:
        message += f"; last error: {last_error!r}"
    raise TimeoutError(message)


async def wait_until_equals(
    fetcher: Callable[[], Awaitable[Any] | Any],
    expected,
    timeout_seconds: float,
    interval_seconds: float = 0.1,
) -> None:
    async def predicate() -> bool:
        value = fetcher()
        if asyncio.iscoroutine(value):
            value = await value
        return value == expected

    await wait_until(
        predicate,
        timeout_seconds,
        interval_seconds,
        description=f"value to equal {expected!r}",
    )


async def wait_until_api_chain_complete(api_client, task_id: str, timeout_seconds: float):
    async def predicate() -> bool:
        response = await api_client.get_dependency_chain(task_id)
        return response.status_code == 200 and response.json().get("complete") is True

    await wait_until(
        predicate,
        timeout_seconds,
        description=f"API dependency chain for {task_id} to become complete",
    )


async def wait_until_mongo_count(mongo_client, expected_count: int, timeout_seconds: float):
    await mongo_client.wait_for_count(expected_count, timeout_seconds)