"""Interpretação pura das configurações de cadastro."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def campos_obrigatorios_atleta(configuracao: Mapping[str, Any]) -> dict[str, bool]:
    rapida = bool(configuracao.get("competicao_rapida"))
    return {
        "foto": bool(configuracao.get("exigir_foto")),
        "instagram": bool(configuracao.get("exigir_instagram")),
        "cpf": not rapida,
    }
