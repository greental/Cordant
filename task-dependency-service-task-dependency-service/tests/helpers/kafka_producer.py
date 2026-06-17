import asyncio
import json
import time
from collections.abc import Callable

from aiokafka import AIOKafkaProducer


class KafkaTestProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer: AIOKafkaProducer | None = None

    async def start(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await self.producer.start()

    async def stop(self):
        if self.producer is not None:
            await self.producer.stop()
            self.producer = None

    def _require_started(self) -> AIOKafkaProducer:
        if self.producer is None:
            raise RuntimeError("KafkaTestProducer is not started")
        return self.producer

    async def send_event(self, event: dict) -> None:
        payload = json.dumps(event).encode("utf-8")
        await self._require_started().send_and_wait(self.topic, payload)

    async def send_raw(self, raw: str | bytes) -> None:
        payload = raw.encode("utf-8") if isinstance(raw, str) else raw
        await self._require_started().send_and_wait(self.topic, payload)

    async def send_events(self, events: list[dict]) -> None:
        for event in events:
            await self.send_event(event)

    async def send_burst(self, events: list[dict | str | bytes], concurrency: int = 100) -> dict:
        started = time.perf_counter()
        semaphore = asyncio.Semaphore(concurrency)

        async def send_one(event: dict | str | bytes) -> None:
            async with semaphore:
                if isinstance(event, dict):
                    await self.send_event(event)
                else:
                    await self.send_raw(event)

        await asyncio.gather(*(send_one(event) for event in events))
        duration_ms = (time.perf_counter() - started) * 1000
        return {
            "sent": len(events),
            "duration_ms": duration_ms,
            "events_per_second": len(events) / (duration_ms / 1000) if duration_ms else 0.0,
        }

    async def send_continuous(
        self,
        event_factory: Callable[[int], dict | str | bytes],
        duration_seconds: int,
        events_per_second: int,
    ) -> dict:
        started = time.perf_counter()
        sent = 0
        interval = 1 / events_per_second if events_per_second > 0 else 0
        next_send = started
        while time.perf_counter() - started < duration_seconds:
            event = event_factory(sent)
            if isinstance(event, dict):
                await self.send_event(event)
            else:
                await self.send_raw(event)
            sent += 1
            next_send += interval
            delay = next_send - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)

        duration_ms = (time.perf_counter() - started) * 1000
        return {
            "sent": sent,
            "duration_ms": duration_ms,
            "events_per_second": sent / (duration_ms / 1000) if duration_ms else 0.0,
        }