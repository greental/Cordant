# Task Dependency Service

A backend service for a project management tool. Users create tasks, and tasks can depend on other tasks.

## Domain

- A **task** has an `id`, `title`, and an optional `parent_task_id`.
- A task with no `parent_task_id` is a root task.
- A task can have many children (tasks that depend on it).
- Tasks are created by publishing events to the Kafka topic `tasks.events`.

## Stack

- **Python 3.11** + **FastAPI**
- **Kafka** for task events
- **MongoDB** for persistence
- **Redis** for caching

## Getting Started

```bash
docker compose up -d
pip install -e .[dev]
uvicorn app.main:app --reload
```

The service will be available at http://localhost:8000.
Swagger UI at http://localhost:8000/docs.

## What's Here

The app boots, connects to its datastores, consumes task events from Kafka,
stores tasks in MongoDB, and exposes task reads plus dependency-chain reads.

- `app/main.py` — FastAPI app entrypoint; creates Mongo indexes and manages the Kafka consumer.
- `app/models.py` — Pydantic API/cache models.
- `app/db.py` — MongoDB and Redis clients.
- `app/repositories.py` — Mongo task repository with idempotent upserts by `id`.
- `app/ingestion.py` — Kafka consumer and task-event handler.
- `app/services.py` — safe dependency-chain resolution.
- `app/cache.py` — Redis cache for complete dependency-chain responses.
- `app/routes/tasks.py` — task and dependency-chain HTTP endpoints.
- `scripts/produce_task_event.py` — helper to publish task events to Kafka.
- `docker-compose.yml` — local Kafka, MongoDB, and Redis.

## Your Task

The work to do is described in **`TASK.md`** as a product ticket — read it first.

## Connection Details

| Service  | Host                  | Port  |
|----------|-----------------------|-------|
| Kafka    | localhost             | 9092  |
| MongoDB  | mongodb://localhost   | 27017 |
| Redis    | redis://localhost     | 6379  |

Database: `taskdb`, Collection: `tasks`.

## Producing a Test Event

Task events are published to `tasks.events`. Each message is a JSON-encoded object with the following shape:

```json
{
  "id": "<uuid>",
  "title": "<string>",
  "parent_task_id": "<uuid or null>"
}
```

Use the helper script:

```bash
python scripts/produce_task_event.py --id root --title "Root task"
python scripts/produce_task_event.py --id child --title "Child task" --parent-task-id root
python scripts/produce_task_event.py --id grandchild --title "Grandchild task" --parent-task-id child
```

The Kafka consumer uses a consumer group and manual offset commits. Valid task
events are committed only after MongoDB persistence succeeds. Invalid JSON or
invalid-shaped events are logged and skipped so a poison message does not block
the stream. Replayed events are safe because writes use MongoDB upsert by `id`.

## API

### Get one task

```bash
curl http://localhost:8000/tasks/child
```

Response shape is unchanged from the scaffold:

```json
{
  "id": "child",
  "title": "Child task",
  "parent_task_id": "root"
}
```

Missing task:

```json
{
  "detail": "Task not found"
}
```

with HTTP `404`.

### Get dependency chain

```bash
curl http://localhost:8000/tasks/grandchild/dependency-chain
```

Response:

```json
{
  "task_id": "grandchild",
  "chain": [
    {"id": "child", "title": "Child task", "parent_task_id": "root"},
    {"id": "root", "title": "Root task", "parent_task_id": null}
  ],
  "complete": true,
  "warnings": []
}
```

The chain is ordered from immediate parent first, then that parent's parent,
continuing up to the root.

Root task response:

```json
{
  "task_id": "root",
  "chain": [],
  "complete": true,
  "warnings": []
}
```

Missing requested task returns HTTP `404`. Bad graph data such as a missing
parent, self-parent, cycle, or max-depth overflow returns HTTP `200` with the
partial chain, `complete: false`, and a structured warning. The resolver always
uses a visited set and max-depth guard so bad data cannot hang the service.

## Demo Workflow

Start dependencies and the app:

```bash
docker compose up -d
pip install -e .[dev]
uvicorn app.main:app --reload
```

Publish a normal root/child/grandchild chain:

```bash
python scripts/produce_task_event.py --id root --title "Root task"
python scripts/produce_task_event.py --id child --title "Child task" --parent-task-id root
python scripts/produce_task_event.py --id grandchild --title "Grandchild task" --parent-task-id child
```

Read it back:

```bash
curl http://localhost:8000/tasks/grandchild
curl http://localhost:8000/tasks/grandchild/dependency-chain
```

Demonstrate out-of-order arrival:

```bash
python scripts/produce_task_event.py --id late-child --title "Late child" --parent-task-id late-parent
curl http://localhost:8000/tasks/late-child/dependency-chain

python scripts/produce_task_event.py --id late-parent --title "Late parent"
curl http://localhost:8000/tasks/late-child/dependency-chain
```

Demonstrate replay/idempotency:

```bash
python scripts/produce_task_event.py --id child --title "Child task" --parent-task-id root
python scripts/produce_task_event.py --id child --title "Child task" --parent-task-id root
curl http://localhost:8000/tasks/child
```

## Redis Caching

MongoDB is the source of truth. Redis is used only as a cache for complete
dependency-chain responses where `complete=true` and `warnings=[]`.

Cached payloads include the current Redis `tasks:data-version`. The version is
incremented after each valid task event is successfully persisted to MongoDB. A
cached chain is served only when its version matches the current version. If Redis
is unavailable or the version cannot be read, the service skips cache and computes
from MongoDB.

## Tests

```bash
pytest tests/unit
docker compose up -d
pytest tests/integration -s
pytest tests/stress -s
```

Unit tests use small fakes only for pure dependency-chain algorithm behavior.
Integration and stress tests use real Docker-backed MongoDB, Redis, Kafka, and
the FastAPI app.

Stress tests are explicit and are not part of the default unit/integration flow.
A reduced local run is:

```bash
STRESS_EVENT_COUNT=1000 STRESS_DURATION_SECONDS=10 STRESS_CHAIN_LENGTH=300 pytest tests/stress -s
```

### Test isolation

- MongoDB integration data uses the `taskdb_test` database by default.
- Redis keys use a per-run prefix such as `test:<run_id>:` and cleanup scans only
  that prefix.
- Kafka tests use unique topics and consumer groups where practical, plus unique
  generated task ids/prefixes.
- Large/stress data is generated dynamically with deterministic prefixes; no
  large JSON fixture files are committed.

### Cache observability

Dependency-chain responses include harmless test/dev observability headers:

- `X-Dependency-Chain-Cache`: `hit`, `miss`, `stale`, or `bypass`
- `X-Dependency-Chain-Source`: `redis` or `mongo`
- `X-Dependency-Chain-Data-Version`: Redis data version when available
- `X-Dependency-Chain-Mongo-Lookups`: lookup count for the request

Cache tests assert these headers instead of relying only on timing.

### Producer scripts

Send one event:

```bash
python scripts/produce_task_event.py --id root --title "Root task"
python scripts/produce_task_event.py --id child --title "Child task" --parent-task-id root
```

Send generated event batches:

```bash
python scripts/produce_task_events.py --count 1000 --shape chain
python scripts/produce_task_events.py --count 1000 --shape out-of-order
python scripts/produce_task_events.py --count 1000 --shape duplicates
python scripts/produce_task_events.py --count 100 --shape malformed
python scripts/produce_task_events.py --count 5000 --shape mixed
```

Both scripts accept `--topic` and `--bootstrap-servers`.

### Seeing test evidence output

Pytest captures `print()` output by default. Use `-s` when you want to see the
human-readable proof summaries printed by integration and stress tests:

```bash
pytest tests/integration -s
pytest tests/stress -s
pytest tests/integration/test_cache_performance.py -s
```

Examples of printed evidence include cache miss/hit source, Mongo lookup counts,
Kafka out-of-order event flow, cycle shapes, and stress latency/throughput
metrics.
