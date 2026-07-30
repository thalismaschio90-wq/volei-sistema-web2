"""Serviço de grupos da competição."""
from repositories import grupos as repo


def criar_tabelas_grupos(*, cache_colunas=None, force=False):
    return repo.criar_tabelas_grupos(cache_colunas=cache_colunas, force=force)


def listar_grupos(competicao):
    return repo.listar_grupos(competicao)


def criar_grupo(nome, competicao, *, fase_travada=False):
    return repo.criar_grupo(nome, competicao, fase_travada=fase_travada)


def adicionar_equipe_no_grupo(grupo_id, equipe, competicao, *, fase_travada=False):
    return repo.adicionar_equipe_no_grupo(grupo_id, equipe, competicao, fase_travada=fase_travada)


def listar_equipes_por_grupo(grupo_id):
    return repo.listar_equipes_por_grupo(grupo_id)


def listar_equipes_por_grupos_competicao(competicao):
    return repo.listar_equipes_por_grupos_competicao(competicao)


def buscar_grupo_por_id(grupo_id, competicao):
    return repo.buscar_grupo_por_id(grupo_id, competicao)


def atualizar_grupo(grupo_id, novo_nome, competicao):
    return repo.atualizar_grupo(grupo_id, novo_nome, competicao)


def remover_equipe_do_grupo(grupo_id, equipe, competicao, *, fase_travada=False):
    return repo.remover_equipe_do_grupo(grupo_id, equipe, competicao, fase_travada=fase_travada)


def excluir_grupo(grupo_id, competicao, *, fase_travada=False):
    return repo.excluir_grupo(grupo_id, competicao, fase_travada=fase_travada)


def limpar_vinculos_competicao(competicao):
    return repo.limpar_vinculos_competicao(competicao)


def substituir_distribuicao_equipes(competicao, distribuicao):
    return repo.substituir_distribuicao_equipes(competicao, distribuicao)
