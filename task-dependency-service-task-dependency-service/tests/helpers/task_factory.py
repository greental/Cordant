import json
import random
from typing import Any


class TaskFactory:
    @staticmethod
    def make_task(
        id: str | None = None,
        title: str | None = None,
        parent_task_id: str | None = None,
    ) -> dict:
        task_id = id or "task"
        return {"id": task_id, "title": title or f"Task {task_id}", "parent_task_id": parent_task_id}

    @staticmethod
    def make_root(id: str = "root") -> dict:
        return TaskFactory.make_task(id=id, title=f"Task {id}")

    @staticmethod
    def make_chain(length: int, prefix: str = "task") -> tuple[list[dict], dict]:
        tasks = []
        for index in range(length):
            task_id = f"{prefix}-{index}"
            parent_id = f"{prefix}-{index - 1}" if index > 0 else None
            tasks.append(TaskFactory.make_task(task_id, f"Task {index}", parent_id))
        final_task_id = tasks[-1]["id"] if tasks else None
        expected_chain_ids = [f"{prefix}-{index}" for index in range(length - 2, -1, -1)]
        return tasks, {
            "root_id": f"{prefix}-0" if tasks else None,
            "final_task_id": final_task_id,
            "expected_chain_ids": expected_chain_ids,
            "expected_unique_count": length,
        }

    @staticmethod
    def make_out_of_order_chain(length: int, prefix: str = "task") -> tuple[list[dict], dict]:
        tasks, expected = TaskFactory.make_chain(length, prefix)
        return list(reversed(tasks)), expected

    @staticmethod
    def make_duplicates(tasks: list[dict], duplicate_count: int) -> tuple[list[dict], dict]:
        duplicated = list(tasks)
        for index in range(duplicate_count):
            duplicated.append(dict(tasks[index % len(tasks)]))
        return duplicated, {
            "expected_unique_count": len({task["id"] for task in tasks}),
            "duplicate_count": duplicate_count,
        }

    @staticmethod
    def make_updates(
        task_id: str,
        old_parent_id: str | None = None,
        new_parent_id: str | None = None,
    ) -> tuple[list[dict], dict]:
        events = [
            TaskFactory.make_task(task_id, "Old title", old_parent_id),
            TaskFactory.make_task(task_id, "Updated title", new_parent_id),
        ]
        return events, {"task_id": task_id, "final_title": "Updated title", "final_parent_id": new_parent_id}

    @staticmethod
    def make_self_parent(id: str = "self") -> dict:
        return TaskFactory.make_task(id, "Self parent", id)

    @staticmethod
    def make_two_node_cycle() -> tuple[list[dict], dict]:
        events = [TaskFactory.make_task("A", "A", "B"), TaskFactory.make_task("B", "B", "A")]
        return events, {"requested_id": "A", "warning_code": "cycle_detected"}

    @staticmethod
    def make_three_node_cycle() -> tuple[list[dict], dict]:
        events = [
            TaskFactory.make_task("A", "A", "B"),
            TaskFactory.make_task("B", "B", "C"),
            TaskFactory.make_task("C", "C", "A"),
        ]
        return events, {"requested_id": "A", "warning_code": "cycle_detected"}

    @staticmethod
    def make_cycle_not_including_requested_directly() -> tuple[list[dict], dict]:
        events = [
            TaskFactory.make_task("A", "A", "B"),
            TaskFactory.make_task("B", "B", "C"),
            TaskFactory.make_task("C", "C", "B"),
        ]
        return events, {
            "requested_id": "A",
            "expected_chain_ids": ["B", "C"],
            "warning_code": "cycle_detected",
        }

    @staticmethod
    def make_missing_parent_child(child_id: str = "child", missing_parent_id: str = "missing") -> dict:
        return TaskFactory.make_task(child_id, "Child", missing_parent_id)

    @staticmethod
    def make_malformed_events() -> list[tuple[str, Any]]:
        long_text = "x" * 10000
        return [
            ("invalid JSON raw string", "{not-json"),
            ("missing id", {"title": "Missing id"}),
            ("missing title", {"id": "missing-title"}),
            ("blank id", {"id": "", "title": "Blank id"}),
            ("blank title", {"id": "blank-title", "title": ""}),
            ("parent_task_id number", {"id": "bad-parent-number", "title": "Bad", "parent_task_id": 123}),
            ("parent_task_id object", {"id": "bad-parent-object", "title": "Bad", "parent_task_id": {}}),
            ("parent_task_id empty string", {"id": "bad-parent-empty", "title": "Bad", "parent_task_id": ""}),
            ("title number", {"id": "title-number", "title": 123}),
            ("title object", {"id": "title-object", "title": {}}),
            ("id number", {"id": 123, "title": "Bad"}),
            ("id object", {"id": {}, "title": "Bad"}),
            ("null title", {"id": "null-title", "title": None}),
            ("null id", {"id": None, "title": "Bad"}),
            ("extremely long title", {"id": "long-title", "title": long_text}),
            ("extremely long id", {"id": long_text, "title": "Long id"}),
            ("unicode title valid case", {"id": "unicode-title", "title": "שלום 🚀"}),
        ]

    @staticmethod
    def make_mixed_burst(
        count: int,
        prefix: str,
        duplicates_rate: float,
        malformed_rate: float,
    ) -> tuple[list[object], dict]:
        rng = random.Random(prefix)
        valid_count = max(1, int(count * (1 - malformed_rate)))
        duplicate_count = int(valid_count * duplicates_rate)
        malformed_count = count - valid_count
        valid, _ = TaskFactory.make_chain(valid_count - duplicate_count, prefix)
        events: list[object] = list(valid)
        for index in range(duplicate_count):
            events.append(dict(valid[index % len(valid)]))
        malformed = TaskFactory.make_malformed_events()
        for index in range(malformed_count):
            _, raw = malformed[index % len(malformed)]
            events.append(json.dumps(raw) if isinstance(raw, dict) else raw)
        rng.shuffle(events)
        return events, {
            "produced_count": len(events),
            "expected_valid_unique_count": len({event["id"] for event in valid}),
            "malformed_count": malformed_count,
            "duplicate_count": duplicate_count,
            "sample_final_task_id": valid[-1]["id"] if valid else None,
        }