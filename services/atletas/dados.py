"""Serviço leve para preparar dados de atletas antes da persistência."""
from __future__ import annotations

from typing import Any

from rules.atletas import DadosAtletaNormalizados, normalizar_dados_atleta


def preparar_dados_atleta(**dados: Any) -> tuple[bool, DadosAtletaNormalizados | None, str]:
    return normalizar_dados_atleta(**dados)
