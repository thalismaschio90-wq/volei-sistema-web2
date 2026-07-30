"""Coordenação do operador e das travas da partida do apontador.

A rota HTTP não precisa conhecer a fachada geral ``banco.py``. Este módulo
isola temporariamente as funções autoritativas de persistência até a extração
definitiva para um repositório próprio.
"""
from __future__ import annotations

from typing import Any, Mapping


def resolver_login_sessao(sessao: Mapping[str, Any] | None) -> str:
    dados = sessao or {}
    return str(
        dados.get("usuario_login")
        or dados.get("login")
        or dados.get("apontador_login")
        or dados.get("usuario")
        or ""
    ).strip()


def validar_schema_oficiais() -> None:
    from core.schema_requirements import require_schema

    require_schema(
        tables=("oficiais", "apontadores_acesso", "competicao_oficiais"),
        context="painel do apontador",
    )


def validar_operador(partida_id: int, competicao: str, operador_login: str, *, renovar: bool = True):
    from banco import validar_operador_partida

    return validar_operador_partida(partida_id, competicao, operador_login, renovar=renovar)


def heartbeat_partida(partida_id: int, competicao: str, operador_login: str, socket_id: str | None = None):
    from banco import heartbeat_partida_operacional

    return heartbeat_partida_operacional(partida_id, competicao, operador_login, socket_id=socket_id)


def liberar_partida(partida_id: int, competicao: str, operador_login: str):
    from banco import liberar_trava_partida_operacional

    return liberar_trava_partida_operacional(partida_id, competicao, operador_login)


def assumir_partida(partida_id: int, competicao: str, operador_login: str, operador_nome: str):
    from banco import assumir_partida_operacional

    return assumir_partida_operacional(partida_id, competicao, operador_login, operador_nome)


def abandonar_partida(partida_id: int, competicao: str, operador_login: str):
    from banco import abandonar_partida_operacional

    return abandonar_partida_operacional(partida_id, competicao, operador_login)


def garantir_pin(competicao: str, apontador_cpf: str):
    from banco import garantir_pin_operacional_apontador

    return garantir_pin_operacional_apontador(competicao, apontador_cpf)


def buscar_vinculo_por_pin(pin: str):
    from banco import buscar_vinculo_operacional_por_pin

    return buscar_vinculo_operacional_por_pin(pin)


__all__ = [
    "abandonar_partida",
    "assumir_partida",
    "buscar_vinculo_por_pin",
    "garantir_pin",
    "heartbeat_partida",
    "liberar_partida",
    "resolver_login_sessao",
    "validar_operador",
    "validar_schema_oficiais",
]
