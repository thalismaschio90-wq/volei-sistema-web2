"""Persistência de escrita do domínio de equipes.

Este módulo concentra operações SQL de edição que antes ficavam no banco.py.
Durante a migração, dependências legadas são resolvidas de forma tardia para
preservar os contratos públicos sem criar importação circular na inicialização.
"""
from __future__ import annotations

from core.security import gerar_hash_senha

from typing import Any

from repositories.conexao import conectar
from repositories.equipes_contexto import gerar_senha_aleatoria


def atualizar_quadro_tecnico_equipe_persistencia(
    nome_equipe: str,
    competicao: str,
    treinador: str,
    auxiliar_tecnico: str,
    preparador_fisico: str,
    medico: str,
):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE equipes
                SET treinador = %s,
                    auxiliar_tecnico = %s,
                    preparador_fisico = %s,
                    medico = %s
                WHERE nome = %s
                  AND competicao = %s
                """,
                (
                    treinador,
                    auxiliar_tecnico,
                    preparador_fisico,
                    medico,
                    nome_equipe,
                    competicao,
                ),
            )
        conn.commit()
    return True, "Atualizado com sucesso!"


def redefinir_senha_da_equipe_persistencia(nome_equipe: str, nome_competicao: str):
    nova_senha = gerar_senha_aleatoria(8)
    nova_senha_hash = gerar_hash_senha(nova_senha)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.login
                FROM equipes_competicoes ec
                JOIN equipes e
                  ON e.login = ec.equipe_login
                  OR LOWER(TRIM(e.nome)) = LOWER(TRIM(ec.equipe_nome))
                WHERE ec.competicao = %s
                  AND (
                        LOWER(TRIM(ec.equipe_nome)) = LOWER(TRIM(%s))
                     OR LOWER(TRIM(e.nome)) = LOWER(TRIM(%s))
                  )
                LIMIT 1
                """,
                (nome_competicao, nome_equipe, nome_equipe),
            )
            equipe = cur.fetchone()
            if not equipe:
                return None
            login_equipe = equipe["login"]
            cur.execute(
                "UPDATE equipes SET senha = %s WHERE login = %s",
                (nova_senha_hash, login_equipe),
            )
            cur.execute(
                """
                UPDATE usuarios
                SET senha = %s
                WHERE login = %s
                  AND perfil = 'equipe'
                """,
                (nova_senha_hash, login_equipe),
            )
        conn.commit()

    return {"login": login_equipe, "senha": nova_senha}


def excluir_equipe_persistencia(nome_equipe: str, nome_competicao: str) -> bool:
    """Remove apenas o vínculo da equipe com a competição atual."""
    nome_equipe = (nome_equipe or "").strip()
    nome_competicao = (nome_competicao or "").strip()
    if not nome_equipe or not nome_competicao:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(travada, FALSE) AS travada FROM competicoes WHERE nome = %s LIMIT 1",
                (nome_competicao,),
            )
            competicao = cur.fetchone() or {}
            if bool(competicao.get("travada")):
                return False
            cur.execute(
                """
                SELECT
                    ec.id,
                    ec.equipe_login,
                    ec.equipe_nome,
                    e.login AS login_global,
                    e.nome AS nome_global
                FROM equipes_competicoes ec
                LEFT JOIN equipes e
                  ON e.login = ec.equipe_login
                  OR LOWER(TRIM(e.nome)) = LOWER(TRIM(ec.equipe_nome))
                WHERE ec.competicao = %s
                  AND (
                        LOWER(TRIM(ec.equipe_nome)) = LOWER(TRIM(%s))
                     OR LOWER(TRIM(e.nome)) = LOWER(TRIM(%s))
                  )
                ORDER BY ec.id
                LIMIT 1
                """,
                (nome_competicao, nome_equipe, nome_equipe),
            )
            vinculo = cur.fetchone()
            if not vinculo:
                return False

            vinculo_id = vinculo.get("id")
            login_equipe = (
                vinculo.get("equipe_login") or vinculo.get("login_global") or ""
            ).strip()
            nome_vinculo = (vinculo.get("equipe_nome") or "").strip()
            nome_global = (vinculo.get("nome_global") or nome_equipe).strip()

            cur.execute(
                "DELETE FROM equipes_competicoes WHERE id = %s AND competicao = %s",
                (vinculo_id, nome_competicao),
            )
            if cur.rowcount <= 0:
                conn.rollback()
                return False

            cur.execute(
                """
                DELETE FROM atletas
                WHERE competicao = %s
                  AND (
                        LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                     OR LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                     OR LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                  )
                """,
                (nome_competicao, nome_equipe, nome_vinculo, nome_global),
            )

            if login_equipe:
                cur.execute(
                    """
                    UPDATE usuarios
                    SET competicao_vinculada = NULL
                    WHERE perfil = 'equipe'
                      AND login = %s
                      AND competicao_vinculada = %s
                    """,
                    (login_equipe, nome_competicao),
                )
            else:
                cur.execute(
                    """
                    UPDATE usuarios
                    SET competicao_vinculada = NULL
                    WHERE perfil = 'equipe'
                      AND competicao_vinculada = %s
                      AND (
                            LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                      )
                    """,
                    (nome_competicao, nome_equipe, nome_vinculo, nome_global),
                )
        conn.commit()
    return True
