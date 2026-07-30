"""Reducer puro do estado operacional da partida."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

from .contracts import EventoJogo
from .validators import inteiro


def _placar(estado: Mapping[str, Any], lado: str) -> int:
    chave_pontos = f"pontos_{lado.lower()}"
    chave_placar = f"placar_{lado.lower()}"
    if chave_pontos in estado:
        return inteiro(estado.get(chave_pontos))
    return inteiro(estado.get(chave_placar))


def aplicar_evento(estado_atual: Mapping[str, Any], evento: EventoJogo) -> dict[str, Any]:
    """Aplica um evento sem acessar Flask, banco, cache ou Socket.IO."""
    estado = deepcopy(dict(estado_atual or {}))
    if evento.tipo != "PONTO_REGISTRADO":
        raise ValueError(f"Evento ainda não suportado: {evento.tipo}")

    lado = str(evento.dados.get("equipe_pontuadora") or "").upper()
    if lado not in {"A", "B"}:
        raise ValueError("Evento de ponto sem equipe válida.")

    novo_placar = _placar(estado, lado) + 1
    estado[f"pontos_{lado.lower()}"] = novo_placar
    estado[f"placar_{lado.lower()}"] = novo_placar
    estado["saque_atual"] = lado
    estado["ultima_acao"] = f"Ponto {lado}"

    if evento.sequencia is not None:
        estado["estado_versao"] = evento.sequencia
        estado["versao"] = evento.sequencia

    return estado
