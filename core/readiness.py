"""Diagnóstico leve de prontidão da aplicação.

O liveness endpoint continua simples. A prontidão verifica dependências reais,
com cache curto para não pressionar PostgreSQL e Redis a cada health check.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable

from core.runtime_config import load_runtime_config

_LOCK = threading.Lock()
_CACHE: dict[str, Any] = {"expires_at": 0.0, "report": None}


def _check_database() -> tuple[bool, str]:
    try:
        from repositories.conexao import conectar
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                cur.fetchone()
        return True, "ok"
    except Exception as exc:  # não expõe URL ou credenciais
        return False, type(exc).__name__


def _check_realtime_store() -> tuple[bool, str, str]:
    try:
        from realtime.state_store import estado_partidas_store
        backend = str(getattr(estado_partidas_store, "backend", "desconhecido"))
        testar = getattr(estado_partidas_store, "testar_conexao", None)
        if callable(testar):
            return bool(testar()), "ok", backend
        return True, "ok", backend
    except Exception as exc:
        return False, type(exc).__name__, "indisponivel"


def build_readiness_report(
    *,
    database_check: Callable[[], tuple[bool, str]] = _check_database,
    realtime_check: Callable[[], tuple[bool, str, str]] = _check_realtime_store,
) -> dict[str, Any]:
    inicio = time.perf_counter()
    runtime = load_runtime_config()
    db_ok, db_detail = database_check()
    realtime_ok, realtime_detail, backend = realtime_check()
    runtime_errors = runtime.errors()
    ok = db_ok and realtime_ok and not runtime_errors
    return {
        "ok": ok,
        "database": {"ok": db_ok, "detail": db_detail},
        "realtime": {"ok": realtime_ok, "detail": realtime_detail, "backend": backend},
        "runtime": runtime.public_dict(),
        "duration_ms": round((time.perf_counter() - inicio) * 1000.0, 2),
        "checked_at": time.time(),
    }


def readiness_report(ttl_seconds: float = 5.0, *, force: bool = False) -> dict[str, Any]:
    agora = time.monotonic()
    if not force:
        report = _CACHE.get("report")
        if report is not None and agora < float(_CACHE.get("expires_at", 0.0)):
            return dict(report)

    with _LOCK:
        agora = time.monotonic()
        if not force:
            report = _CACHE.get("report")
            if report is not None and agora < float(_CACHE.get("expires_at", 0.0)):
                return dict(report)
        report = build_readiness_report()
        _CACHE["report"] = dict(report)
        _CACHE["expires_at"] = agora + max(1.0, float(ttl_seconds))
        return report
