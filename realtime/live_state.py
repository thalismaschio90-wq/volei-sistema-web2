"""Fachada única para o estado operacional vivo das partidas.

Centraliza leitura, gravação, versão e deduplicação de snapshots antes da
publicação. Rotas e Socket.IO deixam de incrementar a versão duas vezes quando
salvam o mesmo estado em sequência (salvar cache + emitir estado).
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from realtime.state_store import EstadoPartidaStore, EstadoVivo, estado_partidas_store

_METADADOS = {"estado_versao", "estado_atualizado_em"}


def _sem_metadados(estado: dict[str, Any] | None) -> dict[str, Any]:
    return {
        chave: valor
        for chave, valor in dict(estado or {}).items()
        if chave not in _METADADOS
    }


@dataclass(frozen=True, slots=True)
class ResultadoPublicacao:
    anterior: EstadoVivo | None
    atual: EstadoVivo
    alterado: bool


class CoordenadorEstadoPartida:
    """Ponto único de acesso ao estado vivo de uma partida."""

    def __init__(self, store: EstadoPartidaStore) -> None:
        self._store = store

    @property
    def store(self) -> EstadoPartidaStore:
        return self._store

    def obter(self, partida_id: object) -> dict[str, Any] | None:
        return self._store.obter(partida_id)

    def obter_com_metadados(self, partida_id: object) -> EstadoVivo | None:
        return self._store.obter_com_metadados(partida_id)

    def versao(self, partida_id: object) -> int:
        return self._store.versao(partida_id)

    def salvar(
        self,
        partida_id: object,
        estado: dict[str, Any],
        *,
        atualizar_origem: bool = False,
    ) -> EstadoVivo | None:
        salvo = self._store.salvar(partida_id, estado)
        if salvo and atualizar_origem and isinstance(estado, dict):
            estado.clear()
            estado.update(copy.deepcopy(salvo.estado))
        return salvo

    def preparar_publicacao(
        self,
        partida_id: object,
        estado: dict[str, Any],
    ) -> ResultadoPublicacao | None:
        """Reutiliza o snapshot atual quando o conteúdo já foi salvo.

        Esse caso ocorre frequentemente quando a rota chama primeiro
        ``atualizar_estado_cache`` e logo depois ``emitir_estado_partida``.
        Antes, a versão era incrementada duas vezes para uma única ação.
        """
        anterior = self._store.obter_com_metadados(partida_id)
        if anterior is not None and _sem_metadados(anterior.estado) == _sem_metadados(estado):
            return ResultadoPublicacao(
                anterior=anterior,
                atual=anterior,
                alterado=False,
            )

        salvo = self._store.salvar(partida_id, estado)
        if salvo is None:
            return None
        return ResultadoPublicacao(
            anterior=anterior,
            atual=salvo,
            alterado=True,
        )

    def remover(self, partida_id: object) -> None:
        self._store.remover(partida_id)

    def limpar(self) -> None:
        self._store.limpar()


estado_partida_vivo = CoordenadorEstadoPartida(estado_partidas_store)
