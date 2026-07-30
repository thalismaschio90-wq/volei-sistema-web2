"""Armazenamento do estado vivo das partidas.

O backend pode ser local (um único processo) ou Redis (múltiplos workers).
A interface é a mesma para rotas, serviços e handlers Socket.IO.
"""
from __future__ import annotations

import copy
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Protocol

from realtime.rooms import normalizar_id_partida

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class EstadoVivo:
    estado: dict[str, Any]
    versao: int
    atualizado_em: float


class EstadoPartidaStore(Protocol):
    def obter(self, partida_id: object) -> dict[str, Any] | None: ...
    def obter_com_metadados(self, partida_id: object) -> EstadoVivo | None: ...
    def versao(self, partida_id: object) -> int: ...
    def salvar(self, partida_id: object, estado: dict[str, Any]) -> EstadoVivo | None: ...
    def salvar_se_aceito(self, partida_id: object, estado: dict[str, Any], avaliador: Any): ...
    def remover(self, partida_id: object) -> None: ...
    def limpar(self) -> None: ...


class LocalEstadoPartidaStore:
    """Store thread-safe para execução com um único processo Gunicorn."""

    backend = "local"

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._estados: dict[str, dict[str, Any]] = {}
        self._versoes: dict[str, int] = {}
        self._atualizados_em: dict[str, float] = {}

    def obter(self, partida_id: object) -> dict[str, Any] | None:
        item = self.obter_com_metadados(partida_id)
        return item.estado if item else None

    def obter_com_metadados(self, partida_id: object) -> EstadoVivo | None:
        chave = normalizar_id_partida(partida_id)
        if not chave:
            return None
        with self._lock:
            estado = self._estados.get(chave)
            if estado is None:
                return None
            return EstadoVivo(
                estado=copy.deepcopy(estado),
                versao=int(self._versoes.get(chave, 0) or 0),
                atualizado_em=float(self._atualizados_em.get(chave, 0.0) or 0.0),
            )

    def versao(self, partida_id: object) -> int:
        chave = normalizar_id_partida(partida_id)
        if not chave:
            return 0
        # A versão não exige copiar o snapshot inteiro (que pode conter atletas,
        # histórico e scout). Lê somente o contador sob o mesmo lock.
        with self._lock:
            return int(self._versoes.get(chave, 0) or 0)

    def salvar(self, partida_id: object, estado: dict[str, Any]) -> EstadoVivo | None:
        chave = normalizar_id_partida(partida_id)
        if not chave:
            return None
        agora = time.time()
        with self._lock:
            versao = int(self._versoes.get(chave, 0) or 0) + 1
            copia = copy.deepcopy(dict(estado or {}))
            copia["estado_versao"] = versao
            copia["estado_atualizado_em"] = agora
            self._estados[chave] = copia
            self._versoes[chave] = versao
            self._atualizados_em[chave] = agora
            return EstadoVivo(copy.deepcopy(copia), versao, agora)

    def salvar_se_aceito(self, partida_id: object, estado: dict[str, Any], avaliador: Any):
        chave = normalizar_id_partida(partida_id)
        if not chave:
            return avaliador({}, 0)
        with self._lock:
            atual = copy.deepcopy(self._estados.get(chave) or {})
            versao_atual = int(self._versoes.get(chave, 0) or 0)
            resultado = avaliador(atual, versao_atual)
            if not getattr(resultado, "aceito", False):
                return resultado

            agora = time.time()
            nova_versao = versao_atual + 1
            copia = copy.deepcopy(dict(estado or {}))
            copia["estado_versao"] = nova_versao
            copia["estado_atualizado_em"] = agora
            self._estados[chave] = copia
            self._versoes[chave] = nova_versao
            self._atualizados_em[chave] = agora

            from realtime.inbound_state import ResultadoEstadoRecebido
            return ResultadoEstadoRecebido(
                aceito=True,
                motivo="aceito",
                estado=copy.deepcopy(copia),
                versao_atual=nova_versao,
                versao_recebida=int(getattr(resultado, "versao_recebida", 0) or 0),
            )

    def remover(self, partida_id: object) -> None:
        chave = normalizar_id_partida(partida_id)
        if not chave:
            return
        with self._lock:
            self._estados.pop(chave, None)
            self._versoes.pop(chave, None)
            self._atualizados_em.pop(chave, None)

    def limpar(self) -> None:
        with self._lock:
            self._estados.clear()
            self._versoes.clear()
            self._atualizados_em.clear()


class RedisEstadoPartidaStore:
    """Store compartilhado entre processos usando Redis e WATCH/MULTI.

    Cada partida ocupa uma única chave JSON. A avaliação da versão e a gravação
    usam transação otimista, impedindo que dois workers gravem sobre a mesma
    versão simultaneamente.
    """

    backend = "redis"

    def __init__(
        self,
        redis_url: str,
        *,
        prefixo: str = "vtp:estado_partida",
        ttl_segundos: int = 86400,
        max_tentativas: int = 8,
        cliente: Any | None = None,
    ) -> None:
        if not redis_url and cliente is None:
            raise ValueError("REDIS_URL não configurada para o estado em Redis.")
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
        self._max_tentativas = max(2, int(max_tentativas or 8))

    def _chave(self, partida_id: object) -> str | None:
        normalizada = normalizar_id_partida(partida_id)
        return f"{self._prefixo}:{normalizada}" if normalizada else None

    @staticmethod
    def _decodificar(valor: Any) -> EstadoVivo | None:
        if not valor:
            return None
        try:
            dados = json.loads(valor) if isinstance(valor, str) else dict(valor)
            estado = dict(dados.get("estado") or {})
            versao = int(dados.get("versao") or estado.get("estado_versao") or 0)
            atualizado_em = float(dados.get("atualizado_em") or estado.get("estado_atualizado_em") or 0.0)
            estado["estado_versao"] = versao
            estado["estado_atualizado_em"] = atualizado_em
            return EstadoVivo(estado=estado, versao=versao, atualizado_em=atualizado_em)
        except (TypeError, ValueError, json.JSONDecodeError):
            logger.exception("Estado inválido encontrado no Redis.")
            return None

    @staticmethod
    def _codificar(estado: dict[str, Any], versao: int, atualizado_em: float) -> str:
        copia = copy.deepcopy(dict(estado or {}))
        copia["estado_versao"] = int(versao)
        copia["estado_atualizado_em"] = float(atualizado_em)
        return json.dumps(
            {"estado": copia, "versao": int(versao), "atualizado_em": float(atualizado_em)},
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )

    def testar_conexao(self) -> bool:
        return bool(self._redis.ping())

    def obter(self, partida_id: object) -> dict[str, Any] | None:
        # A decodificação JSON já cria uma estrutura nova e isolada.
        item = self.obter_com_metadados(partida_id)
        return item.estado if item else None

    def obter_com_metadados(self, partida_id: object) -> EstadoVivo | None:
        chave = self._chave(partida_id)
        if not chave:
            return None
        return self._decodificar(self._redis.get(chave))

    def versao(self, partida_id: object) -> int:
        item = self.obter_com_metadados(partida_id)
        return item.versao if item else 0

    def salvar(self, partida_id: object, estado: dict[str, Any]) -> EstadoVivo | None:
        chave = self._chave(partida_id)
        if not chave:
            return None
        from redis.exceptions import WatchError

        for _ in range(self._max_tentativas):
            with self._redis.pipeline() as pipe:
                try:
                    pipe.watch(chave)
                    atual = self._decodificar(pipe.get(chave))
                    versao = (atual.versao if atual else 0) + 1
                    agora = time.time()
                    payload = self._codificar(estado, versao, agora)
                    pipe.multi()
                    pipe.set(chave, payload, ex=self._ttl)
                    pipe.execute()
                    salvo = self._decodificar(payload)
                    return salvo
                except WatchError:
                    continue
        raise RuntimeError("Não foi possível salvar o estado após conflitos concorrentes no Redis.")

    def salvar_se_aceito(self, partida_id: object, estado: dict[str, Any], avaliador: Any):
        chave = self._chave(partida_id)
        if not chave:
            return avaliador({}, 0)
        from redis.exceptions import WatchError
        from realtime.inbound_state import ResultadoEstadoRecebido

        for _ in range(self._max_tentativas):
            with self._redis.pipeline() as pipe:
                try:
                    pipe.watch(chave)
                    atual_item = self._decodificar(pipe.get(chave))
                    atual = copy.deepcopy(atual_item.estado if atual_item else {})
                    versao_atual = int(atual_item.versao if atual_item else 0)
                    resultado = avaliador(atual, versao_atual)
                    if not getattr(resultado, "aceito", False):
                        pipe.unwatch()
                        return resultado

                    nova_versao = versao_atual + 1
                    agora = time.time()
                    payload = self._codificar(estado, nova_versao, agora)
                    pipe.multi()
                    pipe.set(chave, payload, ex=self._ttl)
                    pipe.execute()
                    salvo = self._decodificar(payload)
                    return ResultadoEstadoRecebido(
                        aceito=True,
                        motivo="aceito",
                        estado=copy.deepcopy(salvo.estado if salvo else dict(estado or {})),
                        versao_atual=nova_versao,
                        versao_recebida=int(getattr(resultado, "versao_recebida", 0) or 0),
                    )
                except WatchError:
                    continue
        raise RuntimeError("Não foi possível aceitar o estado após conflitos concorrentes no Redis.")

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


def criar_estado_partidas_store() -> EstadoPartidaStore:
    backend = str(os.getenv("REALTIME_STATE_BACKEND", "local")).strip().lower()
    if backend not in {"redis", "local", "auto"}:
        logger.warning("REALTIME_STATE_BACKEND=%s inválido; usando local.", backend)
        backend = "local"

    redis_url = str(os.getenv("REDIS_URL") or os.getenv("REALTIME_REDIS_URL") or "").strip()
    if backend == "auto":
        backend = "redis" if redis_url else "local"
        logger.info("Backend automático do estado vivo resolvido para %s.", backend)

    if backend == "redis":
        obrigatorio = _env_bool("REALTIME_REDIS_REQUIRED", False)
        try:
            store = RedisEstadoPartidaStore(
                redis_url,
                prefixo=os.getenv("REALTIME_REDIS_PREFIX", "vtp:estado_partida"),
                ttl_segundos=int(os.getenv("REALTIME_STATE_TTL_SECONDS", "86400")),
            )
            store.testar_conexao()
            logger.info("Estado vivo configurado com Redis.")
            return store
        except Exception:
            if obrigatorio:
                raise
            logger.exception("Redis indisponível; usando store local de compatibilidade.")

    logger.info("Estado vivo configurado em memória local (um worker).")
    return LocalEstadoPartidaStore()


estado_partidas_store = criar_estado_partidas_store()
