"""Regras puras do domínio de equipes.

Este módulo não acessa banco, Flask ou Socket.IO. Ele normaliza e valida os
comandos usados na criação e no vínculo de equipes às competições.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DadosEquipeCompeticao:
    nome_equipe: str
    nome_competicao: str


def normalizar_texto(valor: object) -> str:
    return " ".join(str(valor or "").strip().split())


def preparar_equipe_competicao(nome_equipe: object, nome_competicao: object) -> DadosEquipeCompeticao:
    return DadosEquipeCompeticao(
        nome_equipe=normalizar_texto(nome_equipe),
        nome_competicao=normalizar_texto(nome_competicao),
    )


def validar_equipe_competicao(dados: DadosEquipeCompeticao) -> tuple[bool, str]:
    if not dados.nome_equipe:
        return False, "Informe o nome da equipe."
    if not dados.nome_competicao:
        return False, "Informe a competição."
    return True, ""
