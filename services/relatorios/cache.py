"""Cache curto para relatórios pesados.

Evita recalcular o mesmo relatório quando o usuário abre o preview e logo em
seguida gera o PDF. O backend pode ser local ou Redis. Dados definitivos
continuam no PostgreSQL; este cache é apenas uma otimização descartável.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from cache.local_ttl import CacheTTL


@dataclass(frozen=True, slots=True)
class ResultadoRelatorio:
    titulo: str
    linhas: list[Any]
    cache_hit: bool = False


_LOCAL = CacheTTL()
_REDIS_LOCK = threading.RLock()
_REDIS_CLIENTE: Any | None = None
_VERSOES_LOCK = threading.RLock()
_VERSOES_LOCAIS: dict[str, int] = {}


def _bool_env(nome: str, padrao: bool = False) -> bool:
    valor = str(os.getenv(nome, "1" if padrao else "0") or "").strip().lower()
    return valor in {"1", "true", "yes", "on", "sim"}


def _ttl() -> int:
    try:
        return max(0, int(os.getenv("RELATORIOS_CACHE_TTL_SECONDS", "120") or 120))
    except (TypeError, ValueError):
        return 120


def _backend() -> str:
    valor = str(os.getenv("RELATORIOS_CACHE_BACKEND", "local") or "local").strip().lower()
    return valor if valor in {"local", "redis", "off"} else "local"


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
        import redis
        _REDIS_CLIENTE = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
            health_check_interval=30,
        )
        return _REDIS_CLIENTE


def _competicao_normalizada(competicao: str) -> str:
    return " ".join(str(competicao or "").strip().lower().split())


def _chave_versao_redis(competicao: str) -> str:
    digest = hashlib.sha256(_competicao_normalizada(competicao).encode("utf-8")).hexdigest()
    return f"vtp:relatorio:versao:{digest}"


def versao_cache_competicao(competicao: str) -> int:
    """Retorna a geração atual do cache da competição.

    A versão faz a invalidação ser O(1): chaves antigas expiram pelo TTL e
    novas leituras passam imediatamente a usar outro namespace.
    """
    nome = _competicao_normalizada(competicao)
    if not nome:
        return 0
    if _backend() == "redis":
        cliente = _redis()
        if cliente is not None:
            try:
                valor = cliente.get(_chave_versao_redis(nome))
                return max(0, int(valor or 0))
            except Exception:
                pass
    with _VERSOES_LOCK:
        return int(_VERSOES_LOCAIS.get(nome, 0))


def invalidar_cache_competicao(competicao: str) -> int:
    """Invalida imediatamente todos os relatórios da competição.

    Funciona tanto no backend local quanto no Redis e não precisa varrer
    chaves. Retorna a nova versão para facilitar logs e testes.
    """
    nome = _competicao_normalizada(competicao)
    if not nome:
        return 0
    if _backend() == "redis":
        cliente = _redis()
        if cliente is not None:
            try:
                return int(cliente.incr(_chave_versao_redis(nome)))
            except Exception:
                pass
    with _VERSOES_LOCK:
        nova = int(_VERSOES_LOCAIS.get(nome, 0)) + 1
        _VERSOES_LOCAIS[nome] = nova
        return nova


def chave_relatorio(tipo: str, competicao: str, **filtros: Any) -> str:
    payload = {
        "tipo": str(tipo or "").strip().lower(),
        "competicao": _competicao_normalizada(competicao),
        "versao": versao_cache_competicao(competicao),
        "filtros": {k: str(v or "").strip() for k, v in sorted(filtros.items())},
    }
    bruto = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "vtp:relatorio:" + hashlib.sha256(bruto.encode("utf-8")).hexdigest()


def _obter(chave: str) -> tuple[str, list[Any]] | None:
    backend = _backend()
    if backend == "off" or _ttl() <= 0:
        return None
    if backend == "local":
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
    if not isinstance(valor, dict):
        return None
    titulo = valor.get("titulo")
    linhas = valor.get("linhas")
    if not isinstance(titulo, str) or not isinstance(linhas, list):
        return None
    return titulo, copy.deepcopy(linhas)


def _definir(chave: str, titulo: str, linhas: list[Any]) -> None:
    ttl = _ttl()
    backend = _backend()
    if backend == "off" or ttl <= 0:
        return
    valor = {"titulo": str(titulo), "linhas": copy.deepcopy(list(linhas)), "criado_em": time.time()}
    if backend == "local":
        _LOCAL.definir(chave, valor, ttl)
        return
    cliente = _redis()
    if cliente is None:
        return
    try:
        cliente.setex(chave, ttl, json.dumps(valor, ensure_ascii=False, default=str))
    except Exception:
        return


def gerar_com_cache(
    tipo: str,
    competicao: str,
    gerador: Callable[[], tuple[str, list[Any]]],
    *,
    ignorar_cache: bool = False,
    **filtros: Any,
) -> ResultadoRelatorio:
    chave = chave_relatorio(tipo, competicao, **filtros)
    if not ignorar_cache:
        encontrado = _obter(chave)
        if encontrado:
            return ResultadoRelatorio(encontrado[0], encontrado[1], True)
    titulo, linhas = gerador()
    linhas_lista = list(linhas or [])
    _definir(chave, titulo, linhas_lista)
    return ResultadoRelatorio(str(titulo), linhas_lista, False)


def invalidar_cache_local() -> None:
    """Limpa cache e versões locais; útil nos testes."""
    _LOCAL.limpar()
    with _VERSOES_LOCK:
        _VERSOES_LOCAIS.clear()
