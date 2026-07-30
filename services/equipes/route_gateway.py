"""Gateway temporário da rota de equipes.

A rota HTTP não deve conhecer a fachada gigante ``banco.py``. Este módulo
encaminha diretamente os domínios já extraídos e isola, em wrappers pequenos,
as operações que ainda aguardam migração para serviços/repositórios próprios.
"""
from __future__ import annotations

from typing import Any, Callable

from repositories.conexao import conectar
from repositories.equipes_escrita import (
    atualizar_quadro_tecnico_equipe_persistencia as atualizar_quadro_tecnico_equipe,
    excluir_equipe_persistencia as excluir_equipe,
    redefinir_senha_da_equipe_persistencia as redefinir_senha_da_equipe,
)
from services.competicoes.ciclo import (
    buscar_competicao_por_organizador,
    competicao_esta_travada,
)
from services.competicoes.partidas import listar_partidas, listar_partidas_da_equipe


def _legacy(nome: str) -> Callable[..., Any]:
    """Resolve compatibilidade restante sem acoplar a rota ao módulo legado."""
    from banco import __dict__ as banco_api

    funcao = banco_api.get(nome)
    if not callable(funcao):
        raise AttributeError(f"Função legada indisponível: {nome}")
    return funcao


def _delegar(nome: str, *args: Any, **kwargs: Any) -> Any:
    return _legacy(nome)(*args, **kwargs)


# Operações ainda não extraídas. As assinaturas flexíveis preservam a API atual
# enquanto cada domínio é migrado em sprints posteriores.
def buscar_competicao_por_nome(*args, **kwargs): return _delegar("buscar_competicao_por_nome", *args, **kwargs)
def equipe_existe_na_competicao(*args, **kwargs): return _delegar("equipe_existe_na_competicao", *args, **kwargs)
def buscar_equipes_globais_por_nome(*args, **kwargs): return _delegar("buscar_equipes_globais_por_nome", *args, **kwargs)
def buscar_atleta_global_por_cpf(*args, **kwargs): return _delegar("buscar_atleta_global_por_cpf", *args, **kwargs)
def listar_competicoes_da_equipe_por_login(*args, **kwargs): return _delegar("listar_competicoes_da_equipe_por_login", *args, **kwargs)
def cadastrar_atleta(*args, **kwargs): return _delegar("cadastrar_atleta", *args, **kwargs)
def excluir_atleta(*args, **kwargs): return _delegar("excluir_atleta", *args, **kwargs)
def atualizar_numero_atleta(*args, **kwargs): return _delegar("atualizar_numero_atleta", *args, **kwargs)
def atualizar_atleta_equipe(*args, **kwargs): return _delegar("atualizar_atleta_equipe", *args, **kwargs)
def controle_inscricao_para_equipe(*args, **kwargs): return _delegar("controle_inscricao_para_equipe", *args, **kwargs)
def listar_atletas_da_competicao(*args, **kwargs): return _delegar("listar_atletas_da_competicao", *args, **kwargs)
def atualizar_status_atleta(*args, **kwargs): return _delegar("atualizar_status_atleta", *args, **kwargs)
def aprovar_todos_atletas_pendentes(*args, **kwargs): return _delegar("aprovar_todos_atletas_pendentes", *args, **kwargs)
def salvar_liberacao_extra_equipe(*args, **kwargs): return _delegar("salvar_liberacao_extra_equipe", *args, **kwargs)
def buscar_usuario_por_login(*args, **kwargs): return _delegar("buscar_usuario_por_login", *args, **kwargs)
def validar_edicao_atletas_equipe(*args, **kwargs): return _delegar("validar_edicao_atletas_equipe", *args, **kwargs)
def equipe_tem_partida_iniciada(*args, **kwargs): return _delegar("equipe_tem_partida_iniciada", *args, **kwargs)
def atualizar_dados_conta_usuario(*args, **kwargs): return _delegar("atualizar_dados_conta_usuario", *args, **kwargs)
def escudo_padrao_equipe(*args, **kwargs): return _delegar("escudo_padrao_equipe", *args, **kwargs)
def criar_solicitacao_equipe(*args, **kwargs): return _delegar("criar_solicitacao_equipe", *args, **kwargs)
def listar_solicitacoes_equipes(*args, **kwargs): return _delegar("listar_solicitacoes_equipes", *args, **kwargs)
def responder_solicitacao_equipe(*args, **kwargs): return _delegar("responder_solicitacao_equipe", *args, **kwargs)
def listar_notificacoes_sistema(*args, **kwargs): return _delegar("listar_notificacoes_sistema", *args, **kwargs)
def contar_notificacoes_nao_lidas(*args, **kwargs): return _delegar("contar_notificacoes_nao_lidas", *args, **kwargs)
def criar_notificacao_sistema(*args, **kwargs): return _delegar("criar_notificacao_sistema", *args, **kwargs)
def competicao_eh_rapida(*args, **kwargs): return _delegar("competicao_eh_rapida", *args, **kwargs)
def criar_equipe_temporaria_competicao(*args, **kwargs): return _delegar("criar_equipe_temporaria_competicao", *args, **kwargs)
def cadastrar_atleta_temporario(*args, **kwargs): return _delegar("cadastrar_atleta_temporario", *args, **kwargs)
def atualizar_atleta_temporario(*args, **kwargs): return _delegar("atualizar_atleta_temporario", *args, **kwargs)
def excluir_atleta_temporario(*args, **kwargs): return _delegar("excluir_atleta_temporario", *args, **kwargs)
def excluir_equipe_temporaria_competicao(*args, **kwargs): return _delegar("excluir_equipe_temporaria_competicao", *args, **kwargs)
