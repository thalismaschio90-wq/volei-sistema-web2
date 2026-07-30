"""Estruturas comuns do fluxo em tempo real."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ComandoPartida:
    partida_id: int
    comando_id: str
    tipo: str
    sequencia_esperada: int
    dados: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EventoPartida:
    partida_id: int
    sequencia: int
    tipo: str
    dados: dict[str, Any] = field(default_factory=dict)
