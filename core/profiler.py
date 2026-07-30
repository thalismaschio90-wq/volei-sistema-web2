"""Profiler leve de requisições e funções.

Não armazena argumentos, valores de retorno ou dados de usuário. Apenas nomes
estáticos de seções/funções e durações agregadas.
"""
from __future__ import annotations

import functools
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


@dataclass
class RequestProfile:
    secoes_ms: dict[str, float] = field(default_factory=dict)


_PROFILE: ContextVar[RequestProfile | None] = ContextVar("vtp_request_profile", default=None)


def iniciar_profile() -> None:
    _PROFILE.set(RequestProfile())


def finalizar_profile() -> dict[str, float]:
    profile = _PROFILE.get()
    _PROFILE.set(None)
    return dict(profile.secoes_ms) if profile else {}


def registrar_tempo(nome: str, duracao_ms: float) -> None:
    profile = _PROFILE.get()
    if profile is None:
        return
    chave = str(nome or "outros").strip().lower()[:60] or "outros"
    profile.secoes_ms[chave] = profile.secoes_ms.get(chave, 0.0) + max(0.0, float(duracao_ms))


@contextmanager
def medir_secao(nome: str) -> Iterator[None]:
    inicio = time.perf_counter()
    try:
        yield
    finally:
        registrar_tempo(nome, (time.perf_counter() - inicio) * 1000.0)


def medir_tempo(nome: str | None = None) -> Callable[[F], F]:
    """Decora função e agrega sua duração na seção ``func:<nome>``."""
    def decorator(func: F) -> F:
        secao = f"func:{(nome or func.__qualname__)[:80]}"

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any):
            with medir_secao(secao):
                return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]
    return decorator
