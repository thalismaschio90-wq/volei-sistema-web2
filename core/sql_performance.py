"""Instrumentação segura das consultas PostgreSQL.

A instrumentação é opt-in e não registra parâmetros nem valores do SQL.
Ela mede quantidade, duração total e fingerprints das consultas lentas.
"""
from __future__ import annotations

import hashlib
import inspect
import logging
import os
import re
import time
from pathlib import Path
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from core.sql_advisor import analisar_estrutura_sql
from core.explain import capturar_plano
from core.n_plus_one import detectar_repeticoes, limite_repeticoes_n_plus_one
from core.schema_ddl_guard import validar_sql_sem_ddl

_logger = logging.getLogger("volleytablepro.sql_performance")

_SQL_SPACE_RE = re.compile(r"\s+")
_SQL_LITERAL_RE = re.compile(
    r"'(?:''|[^'])*'|\b\d+(?:\.\d+)?\b",
    flags=re.IGNORECASE,
)


@dataclass
class SqlRequestStats:
    quantidade: int = 0
    duracao_total_ms: float = 0.0
    duracao_max_ms: float = 0.0
    lentas: list[dict[str, Any]] = field(default_factory=list)
    por_fingerprint: dict[str, dict[str, Any]] = field(default_factory=dict)


_STATS: ContextVar[SqlRequestStats | None] = ContextVar("vtp_sql_stats", default=None)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_ORIGIN_EXCLUDED = {
    "core/sql_performance.py",
    "repositories/conexao.py",
    "repositories/transacoes.py",
}


def _origem_consulta() -> str:
    """Retorna apenas arquivo, função e linha do chamador interno.

    Não captura argumentos, locals ou conteúdo SQL. A inspeção ocorre somente
    depois que a consulta ultrapassa o limite de lentidão configurado.
    """
    try:
        for frame_info in inspect.stack(context=0)[2:18]:
            caminho = Path(frame_info.filename).resolve()
            try:
                relativo = caminho.relative_to(_PROJECT_ROOT).as_posix()
            except ValueError:
                continue
            if relativo in _ORIGIN_EXCLUDED or relativo.startswith("tests/"):
                continue
            return f"{relativo}:{frame_info.function}:{frame_info.lineno}"[:300]
    except Exception:
        return ""
    return ""


def _env_bool(nome: str, padrao: bool = False) -> bool:
    valor = os.environ.get(nome)
    if valor is None:
        return padrao
    return valor.strip().lower() in {"1", "true", "yes", "on", "sim"}


def _env_float(nome: str, padrao: float) -> float:
    try:
        return float(os.environ.get(nome, padrao))
    except (TypeError, ValueError):
        return padrao


def instrumentacao_sql_habilitada() -> bool:
    return _env_bool("SQL_PERFORMANCE_LOG_ENABLED", False)


def iniciar_medicao_sql() -> None:
    if instrumentacao_sql_habilitada():
        _STATS.set(SqlRequestStats())
    else:
        _STATS.set(None)


def finalizar_medicao_sql() -> dict[str, Any]:
    stats = _STATS.get()
    _STATS.set(None)
    if stats is None:
        return {"quantidade": 0, "duracao_total_ms": 0.0, "duracao_max_ms": 0.0, "lentas": [], "repetidas": []}
    repetidas = detectar_repeticoes(list(stats.por_fingerprint.values()))
    return {
        "quantidade": int(stats.quantidade),
        "duracao_total_ms": round(stats.duracao_total_ms, 2),
        "duracao_max_ms": round(stats.duracao_max_ms, 2),
        "lentas": list(stats.lentas),
        "repetidas": repetidas,
    }


def _normalizar_sql(sql: Any) -> str:
    texto = str(sql or "")
    texto = _SQL_LITERAL_RE.sub("?", texto)
    texto = _SQL_SPACE_RE.sub(" ", texto).strip()
    return texto[:1000]


def fingerprint_sql(sql: Any) -> tuple[str, str]:
    normalizado = _normalizar_sql(sql)
    digest = hashlib.sha256(normalizado.encode("utf-8", errors="replace")).hexdigest()[:12]
    operacao = normalizado.split(" ", 1)[0].upper() if normalizado else "SQL"
    return digest, operacao


def registrar_consulta(sql: Any, duracao_ms: float, *, executemany: bool = False, plano: dict[str, Any] | None = None) -> None:
    stats = _STATS.get()
    if stats is None:
        return

    stats.quantidade += 1
    stats.duracao_total_ms += float(duracao_ms)
    stats.duracao_max_ms = max(stats.duracao_max_ms, float(duracao_ms))

    fingerprint, operacao = fingerprint_sql(sql)
    repeticao = stats.por_fingerprint.get(fingerprint)
    if repeticao is None:
        repeticao = {
            "fingerprint": fingerprint,
            "operacao": operacao,
            "quantidade": 0,
            "duracao_total_ms": 0.0,
            "duracao_max_ms": 0.0,
            "origem": "",
        }
        stats.por_fingerprint[fingerprint] = repeticao
    repeticao["quantidade"] += 1
    repeticao["duracao_total_ms"] += float(duracao_ms)
    repeticao["duracao_max_ms"] = max(float(repeticao["duracao_max_ms"]), float(duracao_ms))
    if repeticao["quantidade"] == limite_repeticoes_n_plus_one():
        repeticao["origem"] = _origem_consulta()

    limite = max(0.0, _env_float("SQL_SLOW_QUERY_THRESHOLD_MS", 250.0))
    if duracao_ms < limite:
        return

    origem = repeticao.get("origem") or _origem_consulta()
    estrutura = analisar_estrutura_sql(_normalizar_sql(sql))
    item = {
        "fingerprint": fingerprint,
        "operacao": operacao,
        "duracao_ms": round(float(duracao_ms), 2),
        "executemany": bool(executemany),
        "origem": origem,
        "estrutura": estrutura,
        "plano": plano or {},
    }
    limite_itens = max(1, min(20, int(_env_float("SQL_SLOW_QUERY_MAX_PER_REQUEST", 5))))
    if len(stats.lentas) < limite_itens:
        stats.lentas.append(item)

    _logger.warning(
        "sql_lento fingerprint=%s operacao=%s executemany=%s duracao_ms=%.1f origem=%s",
        fingerprint,
        operacao,
        bool(executemany),
        duracao_ms,
        origem or "desconhecida",
    )


class CursorInstrumentado:
    """Proxy transparente de cursor que mede execute/executemany."""

    def __init__(self, cursor: Any, conexao: Any = None):
        self._cursor = cursor
        self._conexao = conexao

    def execute(self, query: Any, params: Any = None, *args: Any, **kwargs: Any):
        validar_sql_sem_ddl(query)
        inicio = time.perf_counter()
        duracao_ms = 0.0
        try:
            if params is None:
                resultado = self._cursor.execute(query, *args, **kwargs)
            else:
                resultado = self._cursor.execute(query, params, *args, **kwargs)
            return self if resultado is self._cursor else resultado
        finally:
            duracao_ms = (time.perf_counter() - inicio) * 1000.0
            limite = max(0.0, _env_float("SQL_SLOW_QUERY_THRESHOLD_MS", 250.0))
            plano = None
            if duracao_ms >= limite and self._conexao is not None:
                plano = capturar_plano(self._conexao, query, params)
            registrar_consulta(query, duracao_ms, plano=plano)

    def executemany(self, query: Any, params_seq: Any, *args: Any, **kwargs: Any):
        validar_sql_sem_ddl(query)
        inicio = time.perf_counter()
        try:
            resultado = self._cursor.executemany(query, params_seq, *args, **kwargs)
            return self if resultado is self._cursor else resultado
        finally:
            registrar_consulta(query, (time.perf_counter() - inicio) * 1000.0, executemany=True)

    def __enter__(self):
        self._cursor.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._cursor.__exit__(exc_type, exc, tb)

    def __iter__(self):
        return iter(self._cursor)

    def __getattr__(self, nome: str):
        return getattr(self._cursor, nome)


class ConexaoInstrumentada:
    """Proxy transparente de conexão que instrumenta apenas os cursores."""

    def __init__(self, conexao: Any):
        self._conexao = conexao

    def cursor(self, *args: Any, **kwargs: Any):
        return CursorInstrumentado(self._conexao.cursor(*args, **kwargs), self._conexao)

    def __enter__(self):
        self._conexao.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._conexao.__exit__(exc_type, exc, tb)

    def __getattr__(self, nome: str):
        return getattr(self._conexao, nome)


def instrumentar_conexao(conexao: Any) -> Any:
    # O proxy também aplica a trava central de DDL, independentemente da
    # instrumentação de desempenho estar habilitada.
    if isinstance(conexao, ConexaoInstrumentada):
        return conexao
    return ConexaoInstrumentada(conexao)
