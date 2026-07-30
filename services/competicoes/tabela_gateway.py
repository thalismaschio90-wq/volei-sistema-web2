"""Dependências de persistência e domínio usadas pela rota da Tabela.

A rota HTTP não deve conhecer a fachada monolítica ``banco.py``. Este módulo
concentra a transição: usa serviços/repositórios diretos nos domínios já
extraídos e mantém poucos adaptadores legados para avanço e link público até
essas funções ganharem serviços próprios.
"""
from __future__ import annotations

from core.schema_inspection import buscar_colunas_tabela as _buscar_colunas_tabela
from repositories.conexao import conectar
from repositories.classificacao_cache import (
    assinatura_classificacao_competicao,
    obter_cache_classificacao,
    salvar_cache_classificacao,
)
from services.competicoes.ciclo import (
    buscar_competicao_por_organizador,
    competicao_esta_travada,
)
from services.competicoes.grupos import (
    criar_grupo,
    listar_grupos,
    adicionar_equipe_no_grupo,
    listar_equipes_por_grupo,
    listar_equipes_por_grupos_competicao,
    remover_equipe_do_grupo,
    excluir_grupo,
)
from services.competicoes.partidas import (
    criar_partida as _criar_partida,
    listar_partidas as _listar_partidas,
    listar_partidas_leve as _listar_partidas_leve,
    listar_estados_resumidos_partidas as _listar_estados_resumidos_partidas,
    buscar_partida_por_id as _buscar_partida_por_id,
    limpar_partidas,
    limpar_partidas_por_fase,
    excluir_partida as _excluir_partida,
    atualizar_partida as _atualizar_partida,
    competicao_tem_partida_iniciada_por_fase,
    fase_pode_ser_alterada,
    proxima_ordem_partida,
)
from services.competicoes.quadras import (
    listar_quadras_competicao,
    garantir_quadras_competicao,
    buscar_quadra_competicao_por_id,
    buscar_quadra_competicao_por_texto,
    formatar_quadra_exibicao,
    normalizar_vinculos_quadras_competicao,
    vincular_grupo_a_quadra,
    aplicar_quadra_em_partida,
)
from services.competicoes.configuracao import (
    buscar_configuracao_agenda as buscar_configuracao_agenda_competicao,
    atualizar_configuracao_agenda as atualizar_configuracao_agenda_competicao,
    inicializar_configuracao_agenda as inicializar_configuracao_agenda_competicao,
)
from services.competicoes.rodadas import buscar_data_hora_rodada_programada


def listar_partidas(competicao):
    return _listar_partidas(competicao, formatar_quadra=formatar_quadra_exibicao)


def listar_partidas_leve(competicao, *, limite=500, offset=0, incluir_escudos=True):
    return _listar_partidas_leve(
        competicao, limite=limite, offset=offset,
        incluir_escudos=incluir_escudos, formatar_quadra=formatar_quadra_exibicao,
    )


def listar_estados_resumidos_partidas(competicao):
    return _listar_estados_resumidos_partidas(competicao)


def buscar_partida_por_id(partida_id, competicao):
    return _buscar_partida_por_id(partida_id, competicao, formatar_quadra=formatar_quadra_exibicao)


def criar_partida(
    competicao,
    grupo,
    equipe_a,
    equipe_b,
    ordem,
    quadra=None,
    fase="grupos",
    data_hora=None,
    rodada=None,
    origem="manual",
    quadra_id=None,
    quadra_nome=None,
):
    return _criar_partida(
        competicao,
        grupo,
        equipe_a,
        equipe_b,
        ordem,
        quadra,
        fase,
        data_hora,
        rodada,
        origem,
        quadra_id,
        quadra_nome,
        buscar_colunas=_buscar_colunas_tabela,
        buscar_quadra_por_id=buscar_quadra_competicao_por_id,
        buscar_quadra_por_texto=buscar_quadra_competicao_por_texto,
        formatar_quadra=formatar_quadra_exibicao,
    )


def atualizar_partida(
    partida_id,
    competicao,
    grupo,
    fase,
    equipe_a,
    equipe_b,
    quadra=None,
    data_hora=None,
    status="aguardando",
    rodada=None,
    quadra_id=None,
    quadra_nome=None,
):
    return _atualizar_partida(
        partida_id,
        competicao,
        grupo,
        fase,
        equipe_a,
        equipe_b,
        quadra,
        data_hora,
        status,
        rodada,
        quadra_id,
        quadra_nome,
        buscar_quadra_por_id=buscar_quadra_competicao_por_id,
        buscar_quadra_por_texto=buscar_quadra_competicao_por_texto,
        formatar_quadra=formatar_quadra_exibicao,
    )


def excluir_partida(partida_id, competicao):
    return _excluir_partida(partida_id, competicao, formatar_quadra=formatar_quadra_exibicao)


def fase_grupos_esta_travada_por_jogo(nome_competicao):
    return competicao_tem_partida_iniciada_por_fase(nome_competicao, "grupos")


def fase_tem_partida_iniciada(nome_competicao, fase):
    return competicao_tem_partida_iniciada_por_fase(nome_competicao, fase)


def fase_partidas_pode_ser_alterada(nome_competicao, fase):
    return fase_pode_ser_alterada(nome_competicao, fase)


# Adaptadores temporários. O isolamento aqui evita que a rota volte a depender
# diretamente de banco.py enquanto avanço e link público são extraídos.
def buscar_competicao_por_nome(nome_competicao):
    from banco import buscar_competicao_por_nome as _fn
    return _fn(nome_competicao)


def buscar_avanco_config_competicao(nome_competicao):
    from banco import buscar_avanco_config_competicao as _fn
    return _fn(nome_competicao)


def gerar_partidas_avanco_competicao(nome_competicao):
    from banco import gerar_partidas_avanco_competicao as _fn
    return _fn(nome_competicao)


def status_avanco_classificatorias_competicao(nome_competicao):
    from banco import status_avanco_classificatorias_competicao as _fn
    return _fn(nome_competicao)


def avanco_ja_gerado_competicao(nome_competicao):
    from banco import avanco_ja_gerado_competicao as _fn
    return _fn(nome_competicao)


def limpar_partidas_avanco_nao_iniciadas_competicao(nome_competicao):
    from banco import limpar_partidas_avanco_nao_iniciadas_competicao as _fn
    return _fn(nome_competicao)


def garantir_codigo_publico_competicao(nome_competicao):
    from banco import garantir_codigo_publico_competicao as _fn
    return _fn(nome_competicao)


def buscar_competicao_por_codigo_publico(codigo_publico):
    from banco import buscar_competicao_por_codigo_publico as _fn
    return _fn(codigo_publico)
