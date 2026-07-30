"""Serviço do ciclo de vida das competições."""
from repositories.competicoes_ciclo import (
    buscar_competicao_por_organizador_persistencia,
    competicao_esta_travada_persistencia,
    competicao_existe_persistencia,
    criar_competicao_com_organizador_persistencia,
    destravar_competicao_persistencia,
    listar_competicoes_do_organizador_persistencia,
    listar_competicoes_persistencia,
    sincronizar_status_competicoes_persistencia,
    travar_competicao_persistencia,
)

sincronizar_status_competicoes = sincronizar_status_competicoes_persistencia
listar_competicoes = listar_competicoes_persistencia
listar_competicoes_do_organizador = listar_competicoes_do_organizador_persistencia
buscar_competicao_por_organizador = buscar_competicao_por_organizador_persistencia
competicao_existe = competicao_existe_persistencia
criar_competicao_com_organizador = criar_competicao_com_organizador_persistencia
competicao_esta_travada = competicao_esta_travada_persistencia
travar_competicao = travar_competicao_persistencia
destravar_competicao = destravar_competicao_persistencia
