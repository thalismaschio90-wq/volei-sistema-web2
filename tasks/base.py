"""Contrato mínimo para tarefas assíncronas futuras."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Tarefa:
    nome: str
    argumentos: dict[str, Any] = field(default_factory=dict)


def executar_sincrono(tarefa: Tarefa, executores: dict[str, Any]) -> Any:
    """Fallback temporário até a adoção de um worker/Redis."""
    executor = executores.get(tarefa.nome)
    if executor is None:
        raise KeyError(f"Tarefa não registrada: {tarefa.nome}")
    return executor(**tarefa.argumentos)
