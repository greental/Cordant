import argparse
import asyncio
import json
import random
import time

from aiokafka import AIOKafkaProducer

from app.config import settings


def make_chain(count: int, prefix: str) -> list[dict]:
    return [make_chain_task(index, prefix) for index in range(count)]


def make_chain_task(index: int, prefix: str) -> dict:
    return {
        "id": f"{prefix}-{index}",
        "title": f"Task {index}",
        "parent_task_id": f"{prefix}-{index - 1}" if index > 0 else None,
    }


def build_events(args) -> list[dict | str]:
    prefix = args.prefix or f"demo-{int(time.time())}"
    if args.shape == "chain":
        return make_chain(args.count, prefix)
    if args.shape == "out-of-order":
        return list(reversed(make_chain(args.count, prefix)))
    if args.shape == "duplicates":
        base = make_chain(args.count, prefix)
        return base + [dict(base[index % len(base)]) for index in range(max(1, args.count // 10))]
    if args.shape == "malformed":
        malformed = [
            "{not-json",
            json.dumps({"id": "missing-title"}),
            json.dumps({"title": "missing id"}),
        ]
        return [malformed[index % len(malformed)] for index in range(args.count)]
    if args.shape in {"mixed", "burst"}:
        base_count = int(args.count * (1 - args.malformed_rate))
        base = make_chain(max(1, base_count), prefix)
        duplicates = [dict(base[index % len(base)]) for index in range(int(args.count * args.duplicates_rate))]
        malformed = ["{not-json" for _ in range(args.count - len(base) - len(duplicates))]
        events: list[dict | str] = base + duplicates + malformed
        random.Random(prefix).shuffle(events)
        return events
    if args.shape == "continuous":
        return []
    raise ValueError(f"Unsupported shape {args.shape}")


async def send_one(producer: AIOKafkaProducer, topic: str, event: dict | str):
    if isinstance(event, dict):
        payload = json.dumps(event).encode("utf-8")
    else:
        payload = event.encode("utf-8")
    await producer.send_and_wait(topic, payload)


async def main_async(args) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=args.bootstrap_servers)
    await producer.start()
    try:
        if args.shape == "continuous":
            prefix = args.prefix or f"continuous-{int(time.time())}"
            interval = 1 / args.events_per_second if args.events_per_second else 0
            deadline = time.perf_counter() + args.duration_seconds
            index = 0
            while time.perf_counter() < deadline:
                await send_one(producer, args.topic, make_chain_task(index, prefix))
                index += 1
                if interval:
                    await asyncio.sleep(interval)
            print(f"Sent {index} continuous events")
            return

        events = build_events(args)
        for event in events:
            await send_one(producer, args.topic, event)
        print(f"Sent {len(events)} events to {args.topic}")
    finally:
        await producer.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish generated task events to Kafka.")
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--shape", choices=["chain", "out-of-order", "duplicates", "malformed", "mixed", "burst", "continuous"], required=True)
    parser.add_argument("--topic", default=settings.kafka_topic)
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--duplicates-rate", type=float, default=0.1)
    parser.add_argument("--malformed-rate", type=float, default=0.01)
    parser.add_argument("--prefix", default=None)
    parser.add_argument("--events-per-second", type=int, default=100)
    parser.add_argument("--duration-seconds", type=int, default=30)
    return parser.parse_args()


def main() -> None:
    asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    main()