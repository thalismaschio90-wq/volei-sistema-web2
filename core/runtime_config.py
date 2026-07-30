"""Validação central das configurações de produção.

Impede combinações inseguras, principalmente múltiplos workers sem Redis
compartilhando o estado vivo e a fila do Socket.IO.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


_TRUE = {"1", "true", "sim", "yes", "on"}


def env_bool(nome: str, padrao: bool = False, env: Mapping[str, str] | None = None) -> bool:
    fonte = os.environ if env is None else env
    valor = str(fonte.get(nome, "")).strip().lower()
    return padrao if not valor else valor in _TRUE


def env_int(
    nome: str,
    padrao: int,
    *,
    minimo: int | None = None,
    maximo: int | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    fonte = os.environ if env is None else env
    try:
        valor = int(str(fonte.get(nome, padrao)).strip())
    except (TypeError, ValueError):
        valor = int(padrao)
    if minimo is not None:
        valor = max(minimo, valor)
    if maximo is not None:
        valor = min(maximo, valor)
    return valor


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    workers: int
    threads: int
    state_backend: str
    socketio_use_redis: bool
    socketio_message_queue_configured: bool
    redis_url_configured: bool
    redis_required: bool

    @property
    def multiple_workers(self) -> bool:
        return self.workers > 1

    @property
    def resolved_state_backend(self) -> str:
        if self.state_backend == "auto":
            return "redis" if self.redis_url_configured else "local"
        return self.state_backend

    @property
    def redis_state_enabled(self) -> bool:
        return self.resolved_state_backend == "redis" and self.redis_url_configured

    @property
    def socket_queue_enabled(self) -> bool:
        return self.socketio_message_queue_configured or (
            self.socketio_use_redis and self.redis_url_configured
        )

    def errors(self) -> list[str]:
        erros: list[str] = []
        if self.state_backend not in {"local", "redis", "auto"}:
            erros.append("REALTIME_STATE_BACKEND deve ser 'local', 'redis' ou 'auto'.")

        if self.state_backend == "redis" and not self.redis_url_configured:
            erros.append("REALTIME_STATE_BACKEND=redis exige REDIS_URL ou REALTIME_REDIS_URL.")

        if self.redis_required and not self.redis_state_enabled:
            erros.append("REALTIME_REDIS_REQUIRED=1 exige estado vivo Redis configurado.")

        if self.multiple_workers:
            if not self.redis_state_enabled:
                erros.append(
                    "GUNICORN_WORKERS acima de 1 exige REALTIME_STATE_BACKEND=redis e REDIS_URL."
                )
            if not self.socket_queue_enabled:
                erros.append(
                    "GUNICORN_WORKERS acima de 1 exige SOCKETIO_USE_REDIS=1 ou SOCKETIO_MESSAGE_QUEUE."
                )
        return erros

    def warnings(self) -> list[str]:
        avisos: list[str] = []
        if self.workers == 1 and self.redis_state_enabled and self.socket_queue_enabled:
            avisos.append(
                "Redis está ativo com um worker. É seguro e indicado para homologação antes de escalar."
            )
        if self.state_backend == "auto" and not self.redis_url_configured:
            avisos.append(
                "Backend em modo auto sem REDIS_URL: usando memória local e mantendo um único worker."
            )
        if self.state_backend == "auto" and self.redis_url_configured:
            avisos.append(
                "Backend em modo auto resolveu para Redis; estado e histórico podem ser compartilhados."
            )
        if self.workers == 1 and self.threads < 4:
            avisos.append("Menos de 4 threads pode limitar requisições simultâneas no modo gthread.")
        if self.workers > 2:
            avisos.append(
                "Mais de 2 workers deve ser adotado somente após medir memória, conexões do banco e carga real."
            )
        return avisos

    def public_dict(self) -> dict[str, object]:
        return {
            "workers": self.workers,
            "threads": self.threads,
            "state_backend": self.state_backend,
            "resolved_state_backend": self.resolved_state_backend,
            "socket_queue_enabled": self.socket_queue_enabled,
            "redis_url_configured": self.redis_url_configured,
            "redis_required": self.redis_required,
            "valid": not self.errors(),
            "errors": self.errors(),
            "warnings": self.warnings(),
        }


def load_runtime_config(env: Mapping[str, str] | None = None) -> RuntimeConfig:
    fonte = os.environ if env is None else env
    redis_url = str(fonte.get("REDIS_URL") or fonte.get("REALTIME_REDIS_URL") or "").strip()
    message_queue = str(fonte.get("SOCKETIO_MESSAGE_QUEUE") or "").strip()
    backend = str(fonte.get("REALTIME_STATE_BACKEND", "local")).strip().lower() or "local"
    return RuntimeConfig(
        workers=env_int("GUNICORN_WORKERS", 1, minimo=1, maximo=8, env=fonte),
        threads=env_int("GUNICORN_THREADS", 4, minimo=1, maximo=32, env=fonte),
        state_backend=backend,
        socketio_use_redis=env_bool("SOCKETIO_USE_REDIS", False, env=fonte),
        socketio_message_queue_configured=bool(message_queue),
        redis_url_configured=bool(redis_url),
        redis_required=env_bool("REALTIME_REDIS_REQUIRED", False, env=fonte),
    )


def assert_runtime_safe(env: Mapping[str, str] | None = None) -> RuntimeConfig:
    config = load_runtime_config(env)
    erros = config.errors()
    if erros:
        raise RuntimeError("Configuração de produção insegura: " + " | ".join(erros))
    return config
