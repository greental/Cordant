import argparse
import asyncio
import json

from aiokafka import AIOKafkaProducer

from app.config import settings


async def produce_event(
    task_id: str,
    title: str,
    parent_task_id: str | None,
    topic: str,
    bootstrap_servers: str,
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        payload = {
            "id": task_id,
            "title": title,
            "parent_task_id": parent_task_id,
        }
        await producer.send_and_wait(topic, json.dumps(payload).encode("utf-8"))
    finally:
        await producer.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a task event to Kafka.")
    parser.add_argument("--id", required=True, dest="task_id", help="Task id")
    parser.add_argument("--title", required=True, help="Task title")
    parser.add_argument("--parent-task-id", default=None, help="Optional parent task id")
    parser.add_argument("--topic", default=settings.kafka_topic, help="Kafka topic")
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers, help="Kafka bootstrap servers")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(
        produce_event(
            args.task_id,
            args.title,
            args.parent_task_id,
            args.topic,
            args.bootstrap_servers,
        )
    )


if __name__ == "__main__":
    main()