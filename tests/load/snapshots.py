from __future__ import annotations

from copy import deepcopy
from typing import Any


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _diff_numeric(before: Any, after: Any) -> Any:
    if isinstance(before, dict) or isinstance(after, dict):
        left = before if isinstance(before, dict) else {}
        right = after if isinstance(after, dict) else {}
        keys = sorted(set(left) | set(right))
        return {key: _diff_numeric(left.get(key), right.get(key)) for key in keys}
    if isinstance(before, (int, float)) or isinstance(after, (int, float)):
        return round(_number(after) - _number(before), 3)
    return deepcopy(after)


def build_snapshot_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Calcula deltas numéricos sem carregar dados sensíveis dos snapshots."""
    return _diff_numeric(before or {}, after or {})


def extract_runtime_summary(payload: dict[str, Any]) -> dict[str, Any]:
    pool = payload.get("pool") if isinstance(payload.get("pool"), dict) else {}
    runtime = payload.get("runtime_config") if isinstance(payload.get("runtime_config"), dict) else {}
    return {
        "ok": bool(payload.get("ok")),
        "backend_estado": payload.get("state_backend") or payload.get("backend_estado") or "",
        "workers": runtime.get("workers") or runtime.get("gunicorn_workers") or 0,
        "threads": runtime.get("threads") or runtime.get("gunicorn_threads") or 0,
        "pool": {
            "abertas": pool.get("conexoes_abertas", pool.get("opened", 0)),
            "em_uso": pool.get("conexoes_em_uso", pool.get("in_use", 0)),
            "espera_ms": pool.get("tempo_espera_total_ms", pool.get("wait_total_ms", 0)),
            "fallbacks": pool.get("fallbacks", pool.get("fallback_count", 0)),
        },
    }
