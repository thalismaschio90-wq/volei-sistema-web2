"""Histórico curto dos deltas do estado vivo para recuperação após reconexão.

O histórico não substitui o snapshot oficial. Ele mantém apenas uma janela
limitada de deltas consecutivos para que clientes que perderam poucas versões
possam recuperar somente o intervalo ausente.
"""
from __future__ import annotations

import copy
import json
import logging
import os
import threading
from collections import deque
from typing import Any, Protocol

from realtime.rooms import normalizar_id_partida

logger = logging.getLogger(__name__)


class HistoricoDeltaStore(Protocol):
    backend: str
    def registrar(self, partida_id: object, delta: dict[str, Any]) -> None: ...
    def recuperar(self, partida_id: object, depois_da_versao: int, *, limite: int = 100) -> list[dict[str, Any]]: ...
    def remover(self, partida_id: object) -> None: ...
    def limpar(self) -> None: ...


def _versao(delta: dict[str, Any], campo: str) -> int:
    try:
        return max(0, int(delta.get(campo) or 0))
    except (TypeError, ValueError):
        return 0


class LocalHistoricoDeltaStore:
    backend = "local"

    def __init__(self, *, max_eventos: int = 200) -> None:
        self._max_eventos = max(10, int(max_eventos or 200))
        self._lock = threading.RLock()
        self._dados: dict[str, deque[dict[str, Any]]] = {}

    def registrar(self, partida_id: object, delta: dict[str, Any]) -> None:
        chave = normalizar_id_partida(partida_id)
        if not chave or not isinstance(delta, dict):
            return
        recebida = _versao(delta, "estado_versao")
        base = _versao(delta, "estado_versao_base")
        if not recebida or recebida <= base:
            return
        copia = copy.deepcopy(delta)
        with self._lock:
            fila = self._dados.setdefault(chave, deque(maxlen=self._max_eventos))
            # Evita duplicidade se uma publicação for repetida pela mesma versão.
            if fila and _versao(fila[-1], "estado_versao") == recebida:
                fila[-1] = copia
            else:
                fila.append(copia)

    def recuperar(self, partida_id: object, depois_da_versao: int, *, limite: int = 100) -> list[dict[str, Any]]:
        chave = normalizar_id_partida(partida_id)
        if not chave:
            return []
        inicio = max(0, int(depois_da_versao or 0))
        maximo = max(1, min(int(limite or 100), self._max_eventos))
        resultado: list[dict[str, Any]] = []
        with self._lock:
            for item in self._dados.get(chave, ()):
                if _versao(item, "estado_versao") <= inicio:
                    continue
                resultado.append(copy.deepcopy(item))
                if len(resultado) >= maximo:
                    break
        return resultado

    def remover(self, partida_id: object) -> None:
        chave = normalizar_id_partida(partida_id)
        if chave:
            with self._lock:
                self._dados.pop(chave, None)

    def limpar(self) -> None:
        with self._lock:
            self._dados.clear()


class RedisHistoricoDeltaStore:
    backend = "redis"

    def __init__(
        self,
        redis_url: str,
        *,
        prefixo: str = "vtp:historico_delta",
        ttl_segundos: int = 86400,
        max_eventos: int = 200,
        cliente: Any | None = None,
    ) -> None:
        if not redis_url and cliente is None:
            raise ValueError("REDIS_URL não configurada para o histórico de deltas.")
        if cliente is None:
            import redis
            cliente = redis.Redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
                health_check_interval=30,
            )
        self._redis = cliente
        self._prefixo = prefixo.rstrip(":")
        self._ttl = max(60, int(ttl_segundos or 86400))
        self._max_eventos = max(10, int(max_eventos or 200))

    def _chave(self, partida_id: object) -> str | None:
        chave = normalizar_id_partida(partida_id)
        return f"{self._prefixo}:{chave}" if chave else None

    def registrar(self, partida_id: object, delta: dict[str, Any]) -> None:
        chave = self._chave(partida_id)
        if not chave or not isinstance(delta, dict):
            return
        recebida = _versao(delta, "estado_versao")
        base = _versao(delta, "estado_versao_base")
        if not recebida or recebida <= base:
            return
        payload = json.dumps(delta, ensure_ascii=False, separators=(",", ":"), default=str)
        with self._redis.pipeline() as pipe:
            pipe.zadd(chave, {payload: recebida})
            # Mantém somente a janela mais recente por posição no sorted set.
            pipe.zremrangebyrank(chave, 0, -(self._max_eventos + 1))
            pipe.expire(chave, self._ttl)
            pipe.execute()

    def recuperar(self, partida_id: object, depois_da_versao: int, *, limite: int = 100) -> list[dict[str, Any]]:
        chave = self._chave(partida_id)
        if not chave:
            return []
        inicio = max(0, int(depois_da_versao or 0))
        maximo = max(1, min(int(limite or 100), self._max_eventos))
        valores = self._redis.zrangebyscore(chave, f"({inicio}", "+inf", start=0, num=maximo)
        resultado: list[dict[str, Any]] = []
        for valor in valores or []:
            try:
                item = json.loads(valor)
                if isinstance(item, dict):
                    resultado.append(item)
            except (TypeError, json.JSONDecodeError):
                logger.warning("Delta inválido ignorado no histórico Redis.")
        resultado.sort(key=lambda item: _versao(item, "estado_versao"))
        return resultado

    def remover(self, partida_id: object) -> None:
        chave = self._chave(partida_id)
        if chave:
            self._redis.delete(chave)

    def limpar(self) -> None:
        cursor = 0
        padrao = f"{self._prefixo}:*"
        while True:
            cursor, chaves = self._redis.scan(cursor=cursor, match=padrao, count=200)
            if chaves:
                self._redis.delete(*chaves)
            if int(cursor) == 0:
                break


def _env_bool(nome: str, padrao: bool = False) -> bool:
    valor = str(os.getenv(nome, "")).strip().lower()
    if not valor:
        return padrao
    return valor in {"1", "true", "sim", "yes", "on"}


def criar_historico_delta_store() -> HistoricoDeltaStore:
    max_eventos = int(os.getenv("REALTIME_RECOVERY_MAX_EVENTS", "200"))
    backend = str(os.getenv("REALTIME_STATE_BACKEND", "local")).strip().lower()
    redis_url = str(os.getenv("REDIS_URL") or os.getenv("REALTIME_REDIS_URL") or "").strip()
    if backend == "auto":
        backend = "redis" if redis_url else "local"
    if backend == "redis":
        obrigatorio = _env_bool("REALTIME_REDIS_REQUIRED", False)
        try:
            store = RedisHistoricoDeltaStore(
                redis_url,
                prefixo=os.getenv("REALTIME_RECOVERY_REDIS_PREFIX", "vtp:historico_delta"),
                ttl_segundos=int(os.getenv("REALTIME_RECOVERY_TTL_SECONDS", "86400")),
                max_eventos=max_eventos,
            )
            store._redis.ping()
            return store
        except Exception:
            if obrigatorio:
                raise
            logger.exception("Redis indisponível para histórico; usando memória local.")
    return LocalHistoricoDeltaStore(max_eventos=max_eventos)


historico_delta_store = criar_historico_delta_store()
