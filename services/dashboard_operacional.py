"""Agregação somente leitura para o dashboard operacional do campeonato."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _status(ok: bool, *, warning: bool = False) -> str:
    if not ok:
        return "critico"
    return "atencao" if warning else "saudavel"


def montar_dashboard_operacional(
    *,
    readiness: dict[str, Any] | None,
    runtime_config: dict[str, Any] | None,
    pool: dict[str, Any] | None,
    performance: dict[str, Any] | None,
    realtime: dict[str, Any] | None,
    presence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    readiness = dict(readiness or {})
    runtime_config = dict(runtime_config or {})
    pool = dict(pool or {})
    performance = dict(performance or {})
    realtime = dict(realtime or {})
    presence = dict(presence or {})

    rotas = list(performance.get("rotas") or [])
    consultas = list(performance.get("consultas_lentas") or [])
    despacho = dict(realtime.get("despacho") or {})
    degradacao = dict(realtime.get("degradacao") or {})

    workers = int(runtime_config.get("workers") or 1)
    backend = str(runtime_config.get("realtime_state_backend") or "local")
    socket_redis = bool(runtime_config.get("socketio_use_redis"))
    redis_ready = backend == "redis" and socket_redis

    rota_p95 = max((_num(item.get("p95_ms")) for item in rotas), default=0.0)
    sql_max = max((_num(item.get("max_ms") or item.get("maior_ms")) for item in consultas), default=0.0)
    pool_wait = _num(pool.get("tempo_espera_medio_ms") or pool.get("espera_media_ms"))
    fila = int(degradacao.get("fila_atual") or despacho.get("fila_atual") or 0)
    modo = str(degradacao.get("modo_atual") or "normal").lower()
    clientes_ativos = int(presence.get("total_clientes") or 0)

    checks = [
        {
            "nome": "Aplicação e dependências",
            "status": _status(bool(readiness.get("ok"))),
            "detalhe": "Readiness aprovado." if readiness.get("ok") else "Há falha em PostgreSQL, Redis ou configuração de runtime.",
        },
        {
            "nome": "Estado em tempo real",
            "status": _status(workers == 1 or redis_ready, warning=(workers == 1 and backend == "local")),
            "detalhe": (
                "Redis compartilhado pronto para múltiplos workers."
                if redis_ready
                else "Estado local: mantenha um único worker."
            ),
        },
        {
            "nome": "Pool PostgreSQL",
            "status": _status(pool_wait < 250, warning=pool_wait >= 100),
            "detalhe": f"Espera média estimada: {pool_wait:.1f} ms.",
        },
        {
            "nome": "Rotas HTTP",
            "status": _status(rota_p95 < 2000, warning=rota_p95 >= 800),
            "detalhe": f"Maior P95 observado: {rota_p95:.1f} ms.",
        },
        {
            "nome": "Consultas SQL",
            "status": _status(sql_max < 1500, warning=sql_max >= 500),
            "detalhe": f"Maior consulta lenta observada: {sql_max:.1f} ms.",
        },
        {
            "nome": "Clientes em tempo real",
            "status": _status(True),
            "detalhe": f"{clientes_ativos} cliente(s) com heartbeat ativo. Backend: {presence.get('backend') or 'local'}.",
        },
        {
            "nome": "Fila Socket.IO",
            "status": _status(modo != "critico" and fila < 100, warning=(modo == "controlado" or fila >= 25)),
            "detalhe": f"Modo: {modo}. Fila atual: {fila}.",
        },
    ]

    ordem = {"critico": 2, "atencao": 1, "saudavel": 0}
    nivel = max((ordem[c["status"]] for c in checks), default=0)
    estado_geral = {0: "saudavel", 1: "atencao", 2: "critico"}[nivel]

    return {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "estado_geral": estado_geral,
        "checks": checks,
        "resumo": {
            "workers": workers,
            "threads": int(runtime_config.get("threads") or 0),
            "backend_estado": backend,
            "socket_redis": socket_redis,
            "pool_espera_media_ms": round(pool_wait, 2),
            "rota_maior_p95_ms": round(rota_p95, 2),
            "sql_maior_ms": round(sql_max, 2),
            "modo_degradacao": modo,
            "fila_realtime": fila,
            "economia_delta_percentual": round(_num(realtime.get("economia_percentual")), 2),
            "clientes_heartbeat": clientes_ativos,
        },
        "top_rotas": rotas[:10],
        "top_consultas": consultas[:10],
        "presenca": presence,
        "realtime": {
            "delta": {k: v for k, v in realtime.items() if k not in {"despacho", "degradacao"}},
            "despacho": despacho,
            "degradacao": degradacao,
        },
    }
