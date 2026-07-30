"""Decisão entre recuperação incremental e snapshot completo."""
from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ResultadoRecuperacao:
    modo: str
    partida_id: str
    versao_solicitada: int
    versao_atual: int
    eventos: tuple[dict[str, Any], ...] = ()
    snapshot: dict[str, Any] | None = None
    motivo: str = ""

    def payload(self) -> dict[str, Any]:
        dados: dict[str, Any] = {
            "ok": True,
            "modo": self.modo,
            "partida_id": self.partida_id,
            "versao_solicitada": self.versao_solicitada,
            "estado_versao": self.versao_atual,
            "motivo": self.motivo,
        }
        if self.eventos:
            dados["eventos"] = [copy.deepcopy(item) for item in self.eventos]
        if self.snapshot is not None:
            dados["snapshot"] = copy.deepcopy(self.snapshot)
        return dados


def _numero(valor: object) -> int:
    try:
        return max(0, int(valor or 0))
    except (TypeError, ValueError):
        return 0


def recuperar_estado(
    partida_id: object,
    versao_cliente: object,
    *,
    state_store: Any,
    history_store: Any,
    limite: int = 100,
) -> ResultadoRecuperacao:
    partida = str(partida_id or "").strip()
    solicitada = _numero(versao_cliente)
    estado_vivo = state_store.obter_com_metadados(partida)
    if estado_vivo is None:
        return ResultadoRecuperacao(
            modo="indisponivel",
            partida_id=partida,
            versao_solicitada=solicitada,
            versao_atual=0,
            motivo="estado_nao_encontrado",
        )

    atual = int(estado_vivo.versao or 0)
    if solicitada >= atual:
        return ResultadoRecuperacao(
            modo="atualizado",
            partida_id=partida,
            versao_solicitada=solicitada,
            versao_atual=atual,
            motivo="cliente_ja_atualizado",
        )

    eventos = history_store.recuperar(partida, solicitada, limite=max(1, int(limite or 100)))
    esperado = solicitada
    validos: list[dict[str, Any]] = []
    for evento in eventos:
        base = _numero(evento.get("estado_versao_base"))
        recebida = _numero(evento.get("estado_versao"))
        if base != esperado or recebida != (base + 1):
            validos = []
            break
        # O history store já devolve objetos isolados do armazenamento. Evita
        # uma segunda cópia profunda de cada delta durante a reconstrução.
        validos.append(evento)
        esperado = recebida
        if esperado >= atual:
            break

    if validos and esperado == atual:
        return ResultadoRecuperacao(
            modo="eventos",
            partida_id=partida,
            versao_solicitada=solicitada,
            versao_atual=atual,
            eventos=tuple(validos),
            motivo="intervalo_contiguo",
        )

    snapshot = copy.deepcopy(estado_vivo.estado)
    snapshot["estado_versao"] = atual
    return ResultadoRecuperacao(
        modo="snapshot",
        partida_id=partida,
        versao_solicitada=solicitada,
        versao_atual=atual,
        snapshot=snapshot,
        motivo="historico_incompleto_ou_lacuna_grande",
    )
