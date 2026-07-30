"""Contexto leve e cacheável do cabeçalho global.

Evita consultar o PostgreSQL em toda renderização de template para descobrir
nome e escudo da equipe logada. O cache nunca é salvo na sessão/cookie.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
from typing import Any, Callable

from cache.local_ttl import CacheTTL

_LOCAL = CacheTTL()
_REDIS_LOCK = threading.RLock()
_REDIS_CLIENTE: Any | None = None
_VERSAO_LOCK = threading.RLock()
_VERSAO_LOCAL = 0


def _backend() -> str:
    valor = str(os.getenv("TOPBAR_CACHE_BACKEND", "local") or "local").strip().lower()
    return valor if valor in {"local", "redis", "off"} else "local"


def _ttl() -> int:
    try:
        return max(0, int(os.getenv("TOPBAR_CACHE_TTL_SECONDS", "60") or 60))
    except (TypeError, ValueError):
        return 60


def _redis():
    global _REDIS_CLIENTE
    if _REDIS_CLIENTE is not None:
        return _REDIS_CLIENTE
    with _REDIS_LOCK:
        if _REDIS_CLIENTE is not None:
            return _REDIS_CLIENTE
        url = str(os.getenv("REDIS_URL", "") or "").strip()
        if not url:
            return None
        try:
            import redis
            _REDIS_CLIENTE = redis.Redis.from_url(
                url,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
                health_check_interval=30,
            )
        except Exception:
            return None
        return _REDIS_CLIENTE


def _versao() -> int:
    if _backend() == "redis":
        cliente = _redis()
        if cliente is not None:
            try:
                return max(0, int(cliente.get("vtp:topbar:versao") or 0))
            except Exception:
                pass
    with _VERSAO_LOCK:
        return _VERSAO_LOCAL


def _chave(login: str, competicao: str | None) -> str:
    bruto = json.dumps(
        {
            "login": str(login or "").strip().lower(),
            "competicao": str(competicao or "").strip().lower(),
            "versao": _versao(),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return "vtp:topbar:" + hashlib.sha256(bruto.encode("utf-8")).hexdigest()


def _obter(chave: str) -> dict[str, Any] | None:
    if _backend() == "off" or _ttl() <= 0:
        return None
    if _backend() == "local":
        valor = _LOCAL.obter(chave)
    else:
        cliente = _redis()
        if cliente is None:
            return None
        try:
            bruto = cliente.get(chave)
            valor = json.loads(bruto) if bruto else None
        except Exception:
            return None
    return copy.deepcopy(valor) if isinstance(valor, dict) else None


def _definir(chave: str, valor: dict[str, Any]) -> None:
    ttl = _ttl()
    if _backend() == "off" or ttl <= 0:
        return
    seguro = copy.deepcopy(valor)
    if _backend() == "local":
        _LOCAL.definir(chave, seguro, ttl)
        return
    cliente = _redis()
    if cliente is not None:
        try:
            cliente.setex(chave, ttl, json.dumps(seguro, ensure_ascii=False, default=str))
        except Exception:
            pass


def buscar_equipe_topbar(
    login: str,
    competicao: str | None,
    buscador: Callable[[str, str | None], dict[str, Any] | None],
) -> dict[str, Any] | None:
    """Busca somente uma vez por TTL e devolve cópia defensiva."""
    login_limpo = str(login or "").strip()
    if not login_limpo:
        return None
    chave = _chave(login_limpo, competicao)
    encontrado = _obter(chave)
    if encontrado is not None:
        return encontrado or None

    equipe = buscador(login_limpo, competicao)
    if not equipe and competicao:
        equipe = buscador(login_limpo, None)
    valor = dict(equipe or {})
    _definir(chave, valor)
    return copy.deepcopy(valor) if valor else None


def invalidar_topbar() -> int:
    """Invalida todos os cabeçalhos após nome, perfil ou escudo mudar."""
    global _VERSAO_LOCAL
    if _backend() == "redis":
        cliente = _redis()
        if cliente is not None:
            try:
                return int(cliente.incr("vtp:topbar:versao"))
            except Exception:
                pass
    with _VERSAO_LOCK:
        _VERSAO_LOCAL += 1
        _LOCAL.limpar()
        return _VERSAO_LOCAL


def limpar_cache_topbar_local() -> None:
    global _VERSAO_LOCAL
    with _VERSAO_LOCK:
        _VERSAO_LOCAL = 0
        _LOCAL.limpar()
