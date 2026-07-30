"""Orquestração do cadastro e vínculo de equipes.

As rotas podem migrar para este serviço gradualmente. Durante a compatibilidade,
o banco.py também delega para estas funções por meio do repositório.
"""
from __future__ import annotations

from repositories.equipes_cadastro import (
    criar_equipe_com_credenciais_persistencia,
    criar_nova_equipe_com_credenciais_persistencia,
    vincular_equipe_a_competicao_persistencia,
    vincular_equipe_existente_competicao_persistencia,
)


def vincular_por_nome(nome_equipe: str, nome_competicao: str, conn=None):
    return vincular_equipe_a_competicao_persistencia(nome_equipe, nome_competicao, conn=conn)


def vincular_por_login(login_equipe: str, nome_competicao: str, conn=None):
    return vincular_equipe_existente_competicao_persistencia(login_equipe, nome_competicao, conn=conn)


def criar_nova(nome_equipe: str, nome_competicao: str):
    return criar_nova_equipe_com_credenciais_persistencia(nome_equipe, nome_competicao)


def criar_ou_vincular(nome_equipe: str, nome_competicao: str):
    return criar_equipe_com_credenciais_persistencia(nome_equipe, nome_competicao)
