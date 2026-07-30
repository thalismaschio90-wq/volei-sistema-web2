"""Persistência das configurações básicas das competições.

As regras e normalizações ficam em ``rules.competicoes_basico``. Este módulo
mantém somente validação de disponibilidade de colunas, SQL e transações.
Enquanto a migração é progressiva, helpers legados são acessados sob demanda
para evitar importação circular com ``banco.py``.
"""
from __future__ import annotations

from core.schema_inspection import buscar_colunas_tabela
from repositories.conexao import conectar
from repositories.competicoes_ciclo import validar_competicao_editavel_persistencia

from rules.competicoes_basico import (
    normalizar_dados_gerais,
    normalizar_estrutura,
    normalizar_pontuacao_desempate,
    normalizar_regras_jogo,
)


def _atualizar_campos(nome_competicao, valores, *, escopo):
    ok_edicao, _ = validar_competicao_editavel_persistencia(nome_competicao, escopo)
    if not ok_edicao:
        return False

    colunas = buscar_colunas_tabela("competicoes")
    disponiveis = {campo: valor for campo, valor in valores.items() if campo in colunas}
    if not disponiveis:
        return True

    sets = [f"{campo} = %s" for campo in disponiveis]
    params = list(disponiveis.values()) + [nome_competicao]
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE competicoes SET {', '.join(sets)} WHERE nome = %s",
                tuple(params),
            )
        conn.commit()
    return True


def atualizar_dados_competicao_persistencia(nome_original, dados):
    valores = normalizar_dados_gerais(dados)
    ok_edicao, _ = validar_competicao_editavel_persistencia(nome_original, "edição")
    if not ok_edicao:
        return False

    colunas = buscar_colunas_tabela("competicoes")
    disponiveis = {campo: valor for campo, valor in valores.items() if campo in colunas}
    if not disponiveis:
        return True

    sets = [f"{campo} = %s" for campo in disponiveis]
    params = list(disponiveis.values()) + [nome_original]
    novo_nome = disponiveis.get("nome")

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE competicoes SET {', '.join(sets)} WHERE nome = %s",
                tuple(params),
            )
            if novo_nome and novo_nome != nome_original:
                cur.execute(
                    "UPDATE usuarios SET competicao_vinculada = %s WHERE competicao_vinculada = %s",
                    (novo_nome, nome_original),
                )
                cur.execute(
                    "UPDATE equipes SET competicao = %s WHERE competicao = %s",
                    (novo_nome, nome_original),
                )
        conn.commit()
    return True


def atualizar_estrutura_competicao_persistencia(nome_competicao, dados):
    return _atualizar_campos(
        nome_competicao,
        normalizar_estrutura(dados),
        escopo="alteração estrutural",
    )


def atualizar_regras_jogo_persistencia(nome_competicao, dados):
    return _atualizar_campos(
        nome_competicao,
        normalizar_regras_jogo(dados),
        escopo="alteração de regras",
    )


def atualizar_pontuacao_desempate_persistencia(nome_competicao, dados):
    return _atualizar_campos(
        nome_competicao,
        normalizar_pontuacao_desempate(dados),
        escopo="alteração de pontuação e desempate",
    )
