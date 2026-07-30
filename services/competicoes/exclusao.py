"""Serviço de exclusão de competições."""
from repositories.competicoes_exclusao import excluir_competicao_persistencia


def excluir_competicao(nome):
    return excluir_competicao_persistencia(nome)
