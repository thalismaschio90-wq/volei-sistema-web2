from __future__ import annotations

from dataclasses import dataclass, field
from math import ceil
from threading import Lock
from time import perf_counter
from typing import Any, Iterable


def percentile(values: Iterable[float], percent: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    percent = max(0.0, min(100.0, float(percent)))
    index = max(0, ceil((percent / 100.0) * len(ordered)) - 1)
    return ordered[index]


@dataclass
class MetricSample:
    name: str
    elapsed_ms: float
    ok: bool = True
    detail: str = ""
    status_code: int | None = None


@dataclass
class MetricCollector:
    _samples: list[MetricSample] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock, repr=False)

    def add(
        self,
        name: str,
        elapsed_ms: float,
        *,
        ok: bool = True,
        detail: str = "",
        status_code: int | None = None,
    ) -> None:
        sample = MetricSample(
            name=str(name),
            elapsed_ms=max(0.0, float(elapsed_ms)),
            ok=bool(ok),
            detail=str(detail or ""),
            status_code=status_code,
        )
        with self._lock:
            self._samples.append(sample)

    def timer(self, name: str) -> "MetricTimer":
        return MetricTimer(self, name)

    def samples(self) -> list[MetricSample]:
        with self._lock:
            return list(self._samples)

    def summary(self) -> dict[str, Any]:
        grouped: dict[str, list[MetricSample]] = {}
        for sample in self.samples():
            grouped.setdefault(sample.name, []).append(sample)

        result: dict[str, Any] = {}
        for name, samples in sorted(grouped.items()):
            latencies = [sample.elapsed_ms for sample in samples]
            failures = [sample for sample in samples if not sample.ok]
            result[name] = {
                "count": len(samples),
                "ok": len(samples) - len(failures),
                "failed": len(failures),
                "error_rate_percent": round((len(failures) / len(samples)) * 100, 3),
                "min_ms": round(min(latencies), 3),
                "avg_ms": round(sum(latencies) / len(latencies), 3),
                "p50_ms": round(percentile(latencies, 50), 3),
                "p95_ms": round(percentile(latencies, 95), 3),
                "p99_ms": round(percentile(latencies, 99), 3),
                "max_ms": round(max(latencies), 3),
            }
        return result


class MetricTimer:
    def __init__(self, collector: MetricCollector, name: str):
        self.collector = collector
        self.name = name
        self.started_at = 0.0
        self.ok = True
        self.detail = ""
        self.status_code: int | None = None

    def __enter__(self) -> "MetricTimer":
        self.started_at = perf_counter()
        return self

    def fail(self, detail: str = "", status_code: int | None = None) -> None:
        self.ok = False
        self.detail = str(detail or "")
        self.status_code = status_code

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if exc is not None:
            self.fail(repr(exc))
        elapsed_ms = (perf_counter() - self.started_at) * 1000.0
        self.collector.add(
            self.name,
            elapsed_ms,
            ok=self.ok,
            detail=self.detail,
            status_code=self.status_code,
        )
        return False
