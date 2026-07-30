"""Fila opcional para relatórios pesados usando RQ/Redis.

O modo síncrono continua sendo o padrão. A fila só é usada quando
RELATORIOS_ASYNC_ENABLED=1 e REDIS_URL está configurada.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


def _bool_env(nome: str, padrao: bool = False) -> bool:
    valor = str(os.getenv(nome, "1" if padrao else "0") or "").strip().lower()
    return valor in {"1", "true", "yes", "on", "sim"}


def fila_habilitada() -> bool:
    return _bool_env("RELATORIOS_ASYNC_ENABLED", False) and bool(str(os.getenv("REDIS_URL", "")).strip())


def nome_fila() -> str:
    return str(os.getenv("RELATORIOS_RQ_QUEUE", "relatorios") or "relatorios").strip()


def timeout_tarefa() -> int:
    try:
        return max(30, int(os.getenv("RELATORIOS_RQ_TIMEOUT_SECONDS", "600") or 600))
    except (TypeError, ValueError):
        return 600


def resultado_ttl() -> int:
    try:
        return max(60, int(os.getenv("RELATORIOS_RQ_RESULT_TTL_SECONDS", "3600") or 3600))
    except (TypeError, ValueError):
        return 3600


def _conexao_redis():
    import redis
    return redis.Redis.from_url(
        os.environ["REDIS_URL"],
        decode_responses=False,
        socket_connect_timeout=3,
        socket_timeout=5,
        health_check_interval=30,
    )


def _queue():
    from rq import Queue
    return Queue(nome_fila(), connection=_conexao_redis(), default_timeout=timeout_tarefa())


@dataclass(frozen=True, slots=True)
class TarefaRelatorio:
    id: str
    status: str
    pronto: bool
    falhou: bool
    mensagem: str = ""
    resultado: dict[str, Any] | None = None


def enfileirar_relatorio(solicitacao: dict[str, Any]) -> str:
    if not fila_habilitada():
        raise RuntimeError("A fila assíncrona de relatórios não está habilitada.")
    from tasks.relatorios import executar_geracao_relatorio

    job = _queue().enqueue(
        executar_geracao_relatorio,
        solicitacao,
        job_timeout=timeout_tarefa(),
        result_ttl=resultado_ttl(),
        failure_ttl=resultado_ttl(),
        description=f"Relatório {solicitacao.get('tipo', '')} - {solicitacao.get('competicao', '')}",
    )
    return str(job.id)


def consultar_tarefa(job_id: str) -> TarefaRelatorio:
    if not fila_habilitada():
        return TarefaRelatorio(str(job_id), "indisponivel", False, True, "Fila assíncrona desabilitada.")
    try:
        from rq.job import Job
        job = Job.fetch(str(job_id), connection=_conexao_redis())
        status = str(job.get_status(refresh=True) or "desconhecido")
        pronto = bool(job.is_finished)
        falhou = bool(job.is_failed or job.is_stopped or job.is_canceled)
        mensagem = ""
        if falhou:
            mensagem = str(job.exc_info or "Falha ao gerar relatório.").splitlines()[-1][:500]
        resultado = job.result if pronto and isinstance(job.result, dict) else None
        return TarefaRelatorio(str(job.id), status, pronto, falhou, mensagem, resultado)
    except Exception as exc:
        return TarefaRelatorio(str(job_id), "nao_encontrada", False, True, str(exc)[:500])
