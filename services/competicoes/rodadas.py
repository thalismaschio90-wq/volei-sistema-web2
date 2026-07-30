"""Serviço de rodadas programadas."""
from repositories import rodadas as repo


def criar_tabela_competicao_rodadas(**kwargs):
    return repo.criar_tabela_competicao_rodadas(**kwargs)


def listar_rodadas_competicao(nome_competicao):
    return repo.listar_rodadas_competicao(nome_competicao)


def salvar_rodadas_competicao(nome_competicao, rodadas, *, validar_edicao=None):
    if validar_edicao:
        ok, _ = validar_edicao(nome_competicao, "alteração das rodadas programadas")
        if not ok:
            return False
    return repo.salvar_rodadas_competicao(nome_competicao, rodadas)


def mapa_rodadas_competicao(nome_competicao):
    return repo.mapa_rodadas_competicao(nome_competicao)


def buscar_data_hora_rodada_programada(nome_competicao, tipo_fase="classificatoria", fase="grupos", serie="", numero_rodada=1):
    return repo.buscar_data_hora_rodada_programada(nome_competicao, tipo_fase, fase, serie, numero_rodada)
