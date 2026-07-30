"""Serviços do perfil e identidade visual das equipes."""
from __future__ import annotations

from services.ui.topbar import invalidar_topbar

from repositories.equipes_perfil import (
    atualizar_escudo_equipe_por_login_persistencia,
    atualizar_nome_equipe_persistencia,
    perfil_equipe_incompleto_por_login_consulta,
    salvar_perfil_equipe_por_login_persistencia,
)


def renomear(nome_atual: str, competicao: str, novo_nome: str):
    resultado = atualizar_nome_equipe_persistencia(nome_atual, competicao, novo_nome)
    if resultado:
        invalidar_topbar()
    return resultado


def salvar_perfil(
    login: str,
    cidade: str = "",
    responsavel: str = "",
    telefone: str = "",
    email: str = "",
    instagram: str = "",
    escudo: str | None = None,
):
    """Salva o perfil preservando a assinatura usada pelas rotas antigas."""
    resultado = salvar_perfil_equipe_por_login_persistencia(
        login,
        cidade,
        responsavel,
        telefone,
        email,
        instagram,
        escudo,
    )
    if resultado:
        invalidar_topbar()
    return resultado


def atualizar_escudo(login: str, escudo: str, escudo_blob: str | None = None):
    resultado = atualizar_escudo_equipe_por_login_persistencia(login, escudo, escudo_blob)
    if resultado:
        invalidar_topbar()
    return resultado


def perfil_incompleto(login: str, conn=None) -> bool:
    return perfil_equipe_incompleto_por_login_consulta(login, conn=conn)
