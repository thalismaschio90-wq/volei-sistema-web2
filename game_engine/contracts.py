"""Contratos imutáveis usados pelo Game Engine experimental."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class ComandoJogo:
    tipo: str
    partida_id: int
    competicao: str
    dados: Mapping[str, Any]
    versao_esperada: int | None = None
    comando_id: str = field(default_factory=lambda: str(uuid4()))


@dataclass(frozen=True, slots=True)
class EventoJogo:
    tipo: str
    partida_id: int
    competicao: str
    dados: Mapping[str, Any]
    comando_id: str
    sequencia: int | None = None
    evento_id: str = field(default_factory=lambda: str(uuid4()))


@dataclass(frozen=True, slots=True)
class ResultadoSombra:
    executado: bool
    divergencias: Mapping[str, Mapping[str, Any]]
    estado_previsto: Mapping[str, Any] | None = None
    motivo: str = ""

    @property
    def divergiu(self) -> bool:
        return bool(self.divergencias)
