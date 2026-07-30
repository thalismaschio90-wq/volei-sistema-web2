"""Consultas de equipes separadas do legado ``banco.py``.

Este módulo contém somente acesso a dados. Regras de campeonato e renderização
não devem ser adicionadas aqui.
"""
from __future__ import annotations

from typing import Any

from .conexao import conectar

_ESCUDO_PADRAO = "/static/img/escudo_padrao.svg"


def listar_equipes_da_competicao(nome_competicao: str) -> list[dict[str, Any]]:
    nome_competicao = (nome_competicao or "").strip()
    if not nome_competicao:
        return []

    sql = """
        WITH vinculos AS (
            SELECT
                ec.*,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        LOWER(TRIM(COALESCE(ec.equipe_login, ''))),
                        LOWER(TRIM(COALESCE(ec.equipe_nome, ''))),
                        ec.competicao
                    ORDER BY ec.id NULLS LAST
                ) AS rn
            FROM equipes_competicoes ec
            WHERE ec.competicao = %s
              AND COALESCE(ec.status, 'ativa') = 'ativa'
        )
        SELECT
            COALESCE(NULLIF(e.nome, ''), v.equipe_nome) AS nome,
            COALESCE(NULLIF(e.login, ''), v.equipe_login) AS login,
            e.senha,
            v.competicao,
            v.equipe_nome AS nome_vinculo,
            v.equipe_login AS login_vinculo,
            v.grupo,
            e.treinador,
            e.auxiliar_tecnico,
            e.preparador_fisico,
            e.medico,
            e.liberacao_extra_inscricao,
            e.liberacao_extra_data,
            e.liberacao_extra_hora,
            e.cidade,
            e.responsavel,
            e.telefone,
            e.email,
            e.instagram,
            COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), %s) AS escudo,
            e.escudo_blob,
            COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), %s) AS escudo_exibicao,
            COALESCE(e.perfil_completo, FALSE) AS perfil_completo,
            v.status AS status_vinculo
        FROM vinculos v
        LEFT JOIN LATERAL (
            SELECT e2.*
            FROM equipes e2
            WHERE
                (
                    COALESCE(v.equipe_login, '') <> ''
                    AND LOWER(TRIM(e2.login)) = LOWER(TRIM(v.equipe_login))
                )
                OR (
                    COALESCE(v.equipe_nome, '') <> ''
                    AND LOWER(TRIM(e2.nome)) = LOWER(TRIM(v.equipe_nome))
                )
            ORDER BY
                CASE
                    WHEN COALESCE(v.equipe_login, '') <> ''
                     AND LOWER(TRIM(e2.login)) = LOWER(TRIM(v.equipe_login)) THEN 0
                    ELSE 1
                END,
                CASE
                    WHEN COALESCE(NULLIF(e2.escudo_blob, ''), NULLIF(e2.escudo, '')) IS NOT NULL THEN 0
                    ELSE 1
                END,
                e2.nome ASC,
                e2.login ASC
            LIMIT 1
        ) e ON TRUE
        WHERE v.rn = 1
        ORDER BY COALESCE(NULLIF(e.nome, ''), v.equipe_nome) ASC
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (nome_competicao, _ESCUDO_PADRAO, _ESCUDO_PADRAO))
            return cur.fetchall()


def buscar_equipe_por_nome_e_competicao(nome_equipe: str, nome_competicao: str) -> dict[str, Any] | None:
    nome_equipe = (nome_equipe or "").strip()
    nome_competicao = (nome_competicao or "").strip()
    if not nome_equipe or not nome_competicao:
        return None

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    e.nome, e.login, e.senha, ec.competicao,
                    e.treinador, e.auxiliar_tecnico, e.preparador_fisico, e.medico,
                    e.liberacao_extra_inscricao, e.liberacao_extra_data, e.liberacao_extra_hora,
                    e.cidade, e.responsavel, e.telefone, e.email, e.instagram,
                    COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), %s) AS escudo,
                    e.escudo_blob,
                    COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), %s) AS escudo_exibicao,
                    COALESCE(e.perfil_completo, FALSE) AS perfil_completo,
                    ec.status AS status_vinculo
                FROM equipes_competicoes ec
                JOIN equipes e ON e.login = ec.equipe_login
                WHERE LOWER(ec.equipe_nome) = LOWER(%s)
                  AND ec.competicao = %s
                LIMIT 1
                """,
                (_ESCUDO_PADRAO, _ESCUDO_PADRAO, nome_equipe, nome_competicao),
            )
            equipe = cur.fetchone()
            if equipe:
                return equipe

            cur.execute(
                """
                SELECT
                    nome, login, senha, competicao,
                    treinador, auxiliar_tecnico, preparador_fisico, medico,
                    liberacao_extra_inscricao, liberacao_extra_data, liberacao_extra_hora,
                    cidade, responsavel, telefone, email, instagram,
                    COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), %s) AS escudo,
                    escudo_blob,
                    COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), %s) AS escudo_exibicao,
                    COALESCE(perfil_completo, FALSE) AS perfil_completo,
                    'ativa' AS status_vinculo
                FROM equipes
                WHERE LOWER(nome) = LOWER(%s)
                  AND competicao = %s
                LIMIT 1
                """,
                (_ESCUDO_PADRAO, _ESCUDO_PADRAO, nome_equipe, nome_competicao),
            )
            return cur.fetchone()


def buscar_equipe_por_login(login: str, competicao_atual: str | None = None) -> dict[str, Any] | None:
    login = (login or "").strip()
    competicao_atual = (competicao_atual or "").strip()
    if not login:
        return None

    with conectar() as conn:
        with conn.cursor() as cur:
            if competicao_atual:
                cur.execute(
                    """
                    SELECT
                        e.nome, e.login, e.senha,
                        e.cidade, e.responsavel, e.telefone, e.email, e.instagram,
                        COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), %s) AS escudo,
                        e.escudo_blob,
                        COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), %s) AS escudo_exibicao,
                        COALESCE(e.perfil_completo, FALSE) AS perfil_completo,
                        ec.competicao, ec.grupo, ec.status AS status_vinculo,
                        e.treinador, e.auxiliar_tecnico, e.preparador_fisico, e.medico,
                        e.liberacao_extra_inscricao, e.liberacao_extra_data, e.liberacao_extra_hora
                    FROM equipes e
                    JOIN equipes_competicoes ec
                      ON ec.equipe_login = e.login
                      OR LOWER(TRIM(e.nome)) = LOWER(TRIM(ec.equipe_nome))
                    WHERE (
                            e.login = %s
                         OR ec.equipe_login = %s
                         OR LOWER(TRIM(ec.equipe_nome)) = LOWER(TRIM(COALESCE((
                                SELECT u.equipe FROM usuarios u WHERE u.login = %s LIMIT 1
                            ), '')))
                    )
                      AND ec.competicao = %s
                      AND COALESCE(ec.status, 'ativa') = 'ativa'
                    LIMIT 1
                    """,
                    (_ESCUDO_PADRAO, _ESCUDO_PADRAO, login, login, login, competicao_atual),
                )
                return cur.fetchone()

            cur.execute(
                """
                SELECT
                    e.nome, e.login, e.senha,
                    e.cidade, e.responsavel, e.telefone, e.email, e.instagram,
                    COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), %s) AS escudo,
                    e.escudo_blob,
                    COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), %s) AS escudo_exibicao,
                    COALESCE(e.perfil_completo, FALSE) AS perfil_completo,
                    NULL::text AS competicao, NULL::text AS grupo, NULL::text AS status_vinculo,
                    e.treinador, e.auxiliar_tecnico, e.preparador_fisico, e.medico,
                    e.liberacao_extra_inscricao, e.liberacao_extra_data, e.liberacao_extra_hora
                FROM equipes e
                WHERE e.login = %s
                LIMIT 1
                """,
                (_ESCUDO_PADRAO, _ESCUDO_PADRAO, login),
            )
            return cur.fetchone()
