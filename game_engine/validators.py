"""Validadores puros do Game Engine experimental."""
from __future__ import annotations

from typing import Any, Mapping


class ErroComandoJogo(ValueError):
    pass


def texto(valor: Any) -> str:
    return str(valor or "").strip()


def inteiro(valor: Any, padrao: int = 0) -> int:
    try:
        return int(valor)
    except (TypeError, ValueError):
        return int(padrao)


def validar_comando_ponto(dados: Mapping[str, Any]) -> dict[str, Any]:
    lado = texto(dados.get("equipe_pontuadora") or dados.get("equipe")).upper()
    if lado not in {"A", "B"}:
        raise ErroComandoJogo("Equipe pontuadora inválida.")
    return {
        "equipe_pontuadora": lado,
        "fundamento": texto(dados.get("detalhe_lance") or dados.get("fundamento")).lower(),
        "atleta_numero": texto(dados.get("atleta_numero")),
        "atleta_nome": texto(dados.get("atleta_nome")),
    }
