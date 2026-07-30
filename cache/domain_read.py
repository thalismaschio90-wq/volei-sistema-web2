"""Cache versionado para leituras de domínio pouco voláteis.

O cache é descartável: PostgreSQL continua sendo a fonte de verdade. A
invalidação usa uma versão por domínio/entidade, evitando varrer chaves no
Redis. O backend local é adequado para um worker; Redis compartilha o cache
entre workers.
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
_VERSOES_LOCK = threading.RLock()
_VERSOES_LOCAIS: dict[str, int] = {}
_MISS = object()


def _backend() -> str:
    valor = str(os.getenv("DOMAIN_READ_CACHE_BACKEND", "local") or "local").strip().lower()
    return valor if valor in {"local", "redis", "off"} else "local"


def _ttl() -> int:
    try:
        return max(0, int(os.getenv("DOMAIN_READ_CACHE_TTL_SECONDS", "60") or 60))
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


def _normalizar(valor: Any) -> str:
    return " ".join(str(valor or "").strip().lower().split())


def _identidade(dominio: str, entidade: Any) -> str:
    return f"{_normalizar(dominio)}::{_normalizar(entidade)}"


def _chave_versao(dominio: str, entidade: Any) -> str:
    bruto = _identidade(dominio, entidade)
    digest = hashlib.sha256(bruto.encode("utf-8")).hexdigest()
    return f"vtp:domain-cache:version:{digest}"


def versao(dominio: str, entidade: Any) -> int:
    identidade = _identidade(dominio, entidade)
    if not identidade.strip(":"):
        return 0
    if _backend() == "redis":
        cliente = _redis()
        if cliente is not None:
            try:
                return max(0, int(cliente.get(_chave_versao(dominio, entidade)) or 0))
            except Exception:
                pass
    with _VERSOES_LOCK:
        return int(_VERSOES_LOCAIS.get(identidade, 0))


def invalidar(dominio: str, entidade: Any) -> int:
    identidade = _identidade(dominio, entidade)
    if not identidade.strip(":"):
        return 0
    if _backend() == "redis":
        cliente = _redis()
        if cliente is not None:
            try:
                return int(cliente.incr(_chave_versao(dominio, entidade)))
            except Exception:
                pass
    with _VERSOES_LOCK:
        nova = int(_VERSOES_LOCAIS.get(identidade, 0)) + 1
        _VERSOES_LOCAIS[identidade] = nova
        return nova


def chave(dominio: str, entidade: Any, operacao: str, **filtros: Any) -> str:
    payload = {
        "dominio": _normalizar(dominio),
        "entidade": _normalizar(entidade),
        "versao": versao(dominio, entidade),
        "operacao": _normalizar(operacao),
        "filtros": {str(k): str(v or "").strip() for k, v in sorted(filtros.items())},
    }
    bruto = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "vtp:domain-cache:" + hashlib.sha256(bruto.encode("utf-8")).hexdigest()


def _obter(chave_cache: str) -> Any:
    if _backend() == "off" or _ttl() <= 0:
        return _MISS
    if _backend() == "local":
        valor = _LOCAL.obter(chave_cache, _MISS)
    else:
        cliente = _redis()
        if cliente is None:
            return _MISS
        try:
            bruto = cliente.get(chave_cache)
            if bruto is None:
                return _MISS
            envelope = json.loads(bruto)
            valor = envelope.get("valor") if isinstance(envelope, dict) else _MISS
        except Exception:
            return _MISS
    return copy.deepcopy(valor) if valor is not _MISS else _MISS


def _definir(chave_cache: str, valor: Any) -> None:
    ttl = _ttl()
    if _backend() == "off" or ttl <= 0:
        return
    seguro = copy.deepcopy(valor)
    if _backend() == "local":
        _LOCAL.definir(chave_cache, seguro, ttl)
        return
    cliente = _redis()
    if cliente is not None:
        try:
            cliente.setex(
                chave_cache,
                ttl,
                json.dumps({"valor": seguro}, ensure_ascii=False, default=str),
            )
        except Exception:
            pass


def obter_ou_carregar(
    dominio: str,
    entidade: Any,
    operacao: str,
    carregador: Callable[[], Any],
    *,
    ignorar_cache: bool = False,
    **filtros: Any,
) -> Any:
    """Retorna cópia cacheada ou executa o carregador uma única vez por TTL."""
    chave_cache = chave(dominio, entidade, operacao, **filtros)
    if not ignorar_cache:
        encontrado = _obter(chave_cache)
        if encontrado is not _MISS:
            return encontrado
    valor = carregador()
    _definir(chave_cache, valor)
    return copy.deepcopy(valor)


def limpar_local() -> None:
    global _REDIS_CLIENTE
    _LOCAL.limpar()
    with _VERSOES_LOCK:
        _VERSOES_LOCAIS.clear()
    _REDIS_CLIENTE = None
