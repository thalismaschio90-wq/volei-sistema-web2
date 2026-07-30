"""Persistência da conferência geral de atletas.

Este módulo não executa migrações durante requisições. A estrutura deve ser
garantida no bootstrap/migração da aplicação.
"""
from __future__ import annotations

from typing import Any

from repositories.conexao import conectar


def buscar_configuracao(nome_competicao: str) -> dict[str, Any] | None:
    nome = str(nome_competicao or "").strip()
    if not nome:
        return None
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    nome,
                    COALESCE(conferencia_liberada, FALSE) AS conferencia_liberada,
                    COALESCE(conferencia_encerrada, FALSE) AS conferencia_encerrada,
                    conferencia_prazo,
                    conferencia_link,
                    COALESCE(aprovacao_automatica_atletas, FALSE) AS aprovacao_automatica_atletas
                FROM competicoes
                WHERE nome = %s
                LIMIT 1
                """,
                (nome,),
            )
            return cur.fetchone()


def listar_atletas(nome_competicao: str) -> list[dict[str, Any]]:
    nome = str(nome_competicao or "").strip()
    if not nome:
        return []
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    equipe,
                    nome,
                    cpf,
                    data_nascimento,
                    numero,
                    status,
                    foto_atleta,
                    instagram
                FROM atletas
                WHERE competicao = %s
                ORDER BY equipe, nome
                """,
                (nome,),
            )
            return cur.fetchall() or []


def salvar_configuracao(
    nome_competicao: str,
    *,
    prazo: str | None,
    link: str | None,
    aprovacao_automatica: bool,
) -> bool:
    nome = str(nome_competicao or "").strip()
    if not nome:
        return False
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE competicoes
                SET conferencia_prazo = %s,
                    conferencia_link = %s,
                    aprovacao_automatica_atletas = %s
                WHERE nome = %s
                """,
                (prazo or "", link or "", bool(aprovacao_automatica), nome),
            )
            alteradas = cur.rowcount
        conn.commit()
    return bool(alteradas)


def definir_status(
    nome_competicao: str,
    *,
    liberada: bool | None = None,
    encerrada: bool | None = None,
) -> bool:
    nome = str(nome_competicao or "").strip()
    if not nome:
        return False

    campos: list[str] = []
    valores: list[Any] = []
    if liberada is not None:
        campos.append("conferencia_liberada = %s")
        valores.append(bool(liberada))
    if encerrada is not None:
        campos.append("conferencia_encerrada = %s")
        valores.append(bool(encerrada))
    if not campos:
        return False

    valores.append(nome)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE competicoes SET {', '.join(campos)} WHERE nome = %s",
                tuple(valores),
            )
            alteradas = cur.rowcount
        conn.commit()
    return bool(alteradas)
