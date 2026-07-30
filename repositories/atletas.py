"""Consultas leves de atletas, sem regras de interface ou Socket.IO."""
from __future__ import annotations

from typing import Any

from .conexao import conectar


def listar_atletas_da_equipe(equipe: str, competicao: str) -> list[dict[str, Any]]:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    nome,
                    cpf,
                    data_nascimento,
                    numero,
                    equipe,
                    competicao,
                    status,
                    equipe_login,
                    equipe_id,
                    foto_atleta,
                    instagram,
                    temporario,
                    capitao_padrao,
                    libero
                FROM atletas
                WHERE equipe = %s AND competicao = %s
                ORDER BY nome
                """,
                (equipe, competicao),
            )
            return cur.fetchall()



def resumir_atletas_da_equipe(equipe: str, competicao: str) -> dict[str, int]:
    """Retorna apenas os contadores usados no painel inicial da equipe.

    Evita carregar fotos, documentos e demais colunas pesadas de todos os
    atletas quando a tela precisa somente dos totais por status.
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE LOWER(TRIM(COALESCE(status, ''))) = 'aprovado') AS aprovados,
                    COUNT(*) FILTER (
                        WHERE LOWER(TRIM(COALESCE(status, ''))) IN
                            ('', 'pendente', 'aguardando', 'em análise', 'em analise', 'em_analise')
                    ) AS pendentes,
                    COUNT(*) FILTER (WHERE LOWER(TRIM(COALESCE(status, ''))) = 'reprovado') AS reprovados
                FROM atletas
                WHERE equipe = %s AND competicao = %s
                """,
                (equipe, competicao),
            )
            row = cur.fetchone() or {}
            return {
                "total": int(row.get("total") or 0),
                "aprovados": int(row.get("aprovados") or 0),
                "pendentes": int(row.get("pendentes") or 0),
                "reprovados": int(row.get("reprovados") or 0),
            }

def listar_atletas_aprovados_da_equipe(equipe: str, competicao: str) -> list[dict[str, Any]]:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    nome,
                    cpf,
                    data_nascimento,
                    numero,
                    equipe,
                    competicao,
                    status,
                    equipe_login,
                    equipe_id,
                    foto_atleta,
                    instagram,
                    temporario,
                    capitao_padrao,
                    libero
                FROM atletas
                WHERE equipe = %s
                  AND competicao = %s
                  AND status = 'aprovado'
                ORDER BY nome
                """,
                (equipe, competicao),
            )
            return cur.fetchall()


def contar_atletas_da_equipe(equipe: str, competicao: str) -> int:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS total
                FROM atletas
                WHERE equipe = %s AND competicao = %s
                """,
                (equipe, competicao),
            )
            row = cur.fetchone()
            return int(row["total"] if row else 0)


def numero_atleta_disponivel(
    numero: int | str | None,
    equipe: str,
    competicao: str,
    id_atleta: int | None = None,
    atleta_id: int | None = None,
) -> bool:
    if id_atleta is None and atleta_id is not None:
        id_atleta = atleta_id
    if numero in (None, ""):
        return True
    try:
        numero_int = int(numero)
    except (TypeError, ValueError):
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            if id_atleta is None:
                cur.execute(
                    """
                    SELECT id FROM atletas
                    WHERE equipe = %s AND competicao = %s AND numero = %s
                    LIMIT 1
                    """,
                    (equipe, competicao, numero_int),
                )
            else:
                cur.execute(
                    """
                    SELECT id FROM atletas
                    WHERE equipe = %s AND competicao = %s AND numero = %s AND id <> %s
                    LIMIT 1
                    """,
                    (equipe, competicao, numero_int, id_atleta),
                )
            return cur.fetchone() is None
