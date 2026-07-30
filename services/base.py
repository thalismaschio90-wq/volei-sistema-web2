"""Tipos comuns para novos serviços."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class ErroDeNegocio(Exception):
    """Erro esperado causado por uma regra do domínio."""


class EstadoDesatualizado(ErroDeNegocio):
    """Comando baseado em uma versão antiga do estado."""


@dataclass(slots=True)
class ResultadoServico:
    ok: bool
    mensagem: str = ""
    dados: dict[str, Any] = field(default_factory=dict)
