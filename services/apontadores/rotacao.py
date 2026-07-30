"""Coordenação do estado de rotação usado pelo apontador."""
from __future__ import annotations

from typing import Any, Mapping

from rules.rotacao import (
    aplicar_recuperacao_saque,
    normalizar_lado_saque,
    normalizar_rotacao,
    substituir_atleta,
)


def transicao_por_ponto(
    *,
    partida: Mapping[str, Any],
    rotacao_a: Any,
    rotacao_b: Any,
    equipe_pontuadora: str,
) -> dict[str, Any]:
    saque_antes = normalizar_lado_saque(
        partida.get("saque_atual") or partida.get("saque_inicial"),
        partida,
    )
    return aplicar_recuperacao_saque(
        rotacao_a=rotacao_a,
        rotacao_b=rotacao_b,
        saque_antes=saque_antes,
        equipe_pontuadora=equipe_pontuadora,
    )


def rotacao_do_estado(estado: Mapping[str, Any] | None, lado: str) -> list[str]:
    estado = estado or {}
    lado = str(lado or "").strip().upper()
    chave = "rotacao_a" if lado == "A" else "rotacao_b"
    rotacao = estado.get(chave)
    if not rotacao and isinstance(estado.get("rotacao"), Mapping):
        rotacao = estado["rotacao"].get("equipe_a" if lado == "A" else "equipe_b")
    return normalizar_rotacao(rotacao)


def aplicar_substituicao_estado(
    estado: Mapping[str, Any] | None,
    *,
    equipe: str,
    numero_sai: Any,
    numero_entra: Any,
) -> dict[str, Any]:
    base = estado or {}
    equipe_normalizada = str(equipe or "").strip().upper()
    chave = "rotacao_a" if equipe_normalizada == "A" else "rotacao_b"
    rotacao_atual = base.get(chave)
    rotacao_nova = substituir_atleta(rotacao_atual, numero_sai, numero_entra)
    novo = dict(base)
    novo[chave] = rotacao_nova
    return novo
