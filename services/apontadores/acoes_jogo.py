"""Serviço do apontador para tempos e ações disciplinares."""

from __future__ import annotations

from typing import Any, Mapping

from rules.acoes_jogo import (
    ErroAcaoJogo,
    aplicar_acao_local,
    descricao_acao,
    normalizar_equipe,
    validar_cartao_verde,
    validar_retardamento,
    validar_sancao,
    validar_tempo,
)


def preparar_tempo(equipe: Any, estado: Mapping[str, Any] | None, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    lado, usados, limite = validar_tempo(equipe, estado)
    dados = dict(payload or {})
    dados["duracao"] = _inteiro(dados.get("duracao"), 30)
    return {
        "equipe": lado,
        "payload": dados,
        "usados_antes": usados,
        "limite": limite,
    }


def preparar_retardamento(equipe: Any, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {"equipe": validar_retardamento(equipe), "payload": dict(payload or {})}


def preparar_sancao(equipe: Any, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    lado, alvo = validar_sancao(equipe, payload)
    dados = dict(payload or {})
    dados.update(alvo)
    return {"equipe": lado, "payload": dados}


def preparar_cartao_verde(equipe: Any, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    lado, alvo = validar_cartao_verde(equipe, payload)
    dados = dict(payload or {})
    dados.update(alvo)
    return {"equipe": lado, "payload": dados}


def aplicar_local(estado: Mapping[str, Any] | None, tipo: str, equipe: Any, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return aplicar_acao_local(estado, tipo, equipe, payload)


def descrever(tipo: str, equipe: Any = "", payload: Mapping[str, Any] | None = None) -> str:
    return descricao_acao(tipo, equipe, payload)


def normalizar_lado(equipe: Any) -> str:
    return normalizar_equipe(equipe)


def _inteiro(valor: Any, padrao: int = 0) -> int:
    try:
        return int(valor)
    except (TypeError, ValueError):
        return padrao


__all__ = [
    "ErroAcaoJogo",
    "aplicar_local",
    "descrever",
    "normalizar_lado",
    "preparar_cartao_verde",
    "preparar_retardamento",
    "preparar_sancao",
    "preparar_tempo",
]
