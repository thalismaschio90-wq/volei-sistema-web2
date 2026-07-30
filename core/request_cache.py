"""Cache efêmero por contexto de execução.

Usado para eliminar consultas repetidas dentro da mesma operação (por exemplo,
a geração de um relatório). O cache não atravessa requisições e nunca substitui
Redis ou o cache de domínio.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from typing import Any, Hashable, Iterator

_CACHE: ContextVar[dict[Hashable, Any] | None] = ContextVar("vtp_request_cache", default=None)


def ativo() -> bool:
    return _CACHE.get() is not None


def obter(chave: Hashable, padrao: Any = None) -> Any:
    cache = _CACHE.get()
    if cache is None or chave not in cache:
        return padrao
    return deepcopy(cache[chave])


def armazenar(chave: Hashable, valor: Any) -> Any:
    cache = _CACHE.get()
    if cache is not None:
        cache[chave] = deepcopy(valor)
    return valor


@contextmanager
def escopo_cache() -> Iterator[dict[Hashable, Any]]:
    """Ativa um cache isolado e o limpa automaticamente ao sair."""
    existente = _CACHE.get()
    if existente is not None:
        yield existente
        return

    cache: dict[Hashable, Any] = {}
    token = _CACHE.set(cache)
    try:
        yield cache
    finally:
        _CACHE.reset(token)
