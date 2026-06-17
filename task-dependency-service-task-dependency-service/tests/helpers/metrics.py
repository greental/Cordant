import time


class Timer:
    def __enter__(self) -> "Timer":
        self.start = time.perf_counter()
        self.end = self.start
        self.duration_ms = 0.0
        return self

    def __exit__(self, *args) -> None:
        self.end = time.perf_counter()
        self.duration_ms = (self.end - self.start) * 1000


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * p / 100
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def summarize_latencies(durations_ms: list[float]) -> dict:
    return {
        "count": len(durations_ms),
        "p50_ms": percentile(durations_ms, 50),
        "p95_ms": percentile(durations_ms, 95),
        "max_ms": max(durations_ms) if durations_ms else 0.0,
        "durations_ms": durations_ms,
    }


def print_stress_summary(name: str, metrics: dict) -> None:
    print(f"\n{name}")
    print("=" * len(name))
    for key, value in metrics.items():
        if key == "durations_ms":
            continue
        print(f"{key}: {value}")


def print_test_summary(name: str, details: dict) -> None:
    print(f"\n[{name}]")
    for key, value in details.items():
        print(f"{key}: {value}")