"""Infraestrutura compartilhada pelos repositórios do domínio de equipes.

Este módulo não depende de ``banco.py``. Ele centraliza somente utilitários de
persistência e compatibilidade de schema usados por cadastro, escrita e perfil.
"""
from __future__ import annotations

import random
import re
import string
from typing import Any

from core.schema_inspection import buscar_colunas_tabela
from core.schema_requirements import require_schema
from repositories.conexao import conectar


_CARACTERES_SENHA = string.ascii_uppercase + string.digits


def validar_schema_equipes(*, contexto: str = "equipes") -> None:
    require_schema(
        tables=("equipes", "equipes_competicoes", "usuarios", "competicoes"),
        columns={
            "equipes": (
                "nome", "login", "senha", "competicao", "treinador",
                "auxiliar_tecnico", "preparador_fisico", "medico",
                "liberacao_extra_inscricao", "liberacao_extra_data",
                "liberacao_extra_hora", "cidade", "responsavel", "telefone",
                "email", "instagram", "escudo", "escudo_blob",
                "perfil_completo", "cliente_id",
            ),
            "equipes_competicoes": (
                "id", "equipe_login", "equipe_nome", "competicao", "status",
                "cliente_id",
            ),
            "usuarios": (
                "login", "nome", "senha", "perfil", "ativo", "equipe",
                "competicao_vinculada", "cliente_id",
            ),
            "competicoes": ("nome", "cliente_id", "travada"),
        },
        context=contexto,
    )


def cliente_id_por_competicao(competicao: object, conn=None) -> int | None:
    nome = str(competicao or "").strip()
    if not nome:
        return None

    def _executar(cnx):
        with cnx.cursor() as cur:
            cur.execute(
                """
                SELECT cliente_id
                FROM competicoes
                WHERE TRIM(LOWER(nome)) = TRIM(LOWER(%s))
                LIMIT 1
                """,
                (nome,),
            )
            row = cur.fetchone() or {}
            return row.get("cliente_id") if hasattr(row, "get") else row[0]

    try:
        if conn is not None:
            return _executar(conn)
        with conectar() as cnx:
            return _executar(cnx)
    except Exception:
        return None


def normalizar_login_equipe(nome_equipe: object) -> str:
    texto = str(nome_equipe or "").lower().strip()
    texto = re.sub(r"[^\w\s]", "", texto)
    texto = re.sub(r"\s+", "_", texto)[:24].strip("_") or "cadastro"
    return f"eq_{texto}"


def gerar_senha_aleatoria(tamanho: int = 8) -> str:
    tamanho = max(4, int(tamanho or 8))
    return "".join(random.choice(_CARACTERES_SENHA) for _ in range(tamanho))


def gerar_login_unico(base: object, conn=None) -> str:
    base_limpa = str(base or "").strip() or "eq_cadastro"

    def _executar(cnx):
        login = base_limpa
        contador = 1
        with cnx.cursor() as cur:
            while True:
                cur.execute("SELECT 1 FROM usuarios WHERE login = %s LIMIT 1", (login,))
                if cur.fetchone() is None:
                    return login
                contador += 1
                login = f"{base_limpa}_{contador}"

    if conn is not None:
        return _executar(conn)
    with conectar() as cnx:
        return _executar(cnx)


def buscar_equipe_global_por_nome(
    nome_equipe: object,
    *,
    conn=None,
    cliente_id: int | None = None,
    competicao: object = None,
) -> dict[str, Any] | None:
    nome = str(nome_equipe or "").strip()
    if not nome:
        return None
    cid = cliente_id if cliente_id is not None else cliente_id_por_competicao(competicao, conn=conn)
    sql = """
        SELECT
            id, nome, login, senha, competicao, treinador, auxiliar_tecnico,
            preparador_fisico, medico, liberacao_extra_inscricao,
            liberacao_extra_data, liberacao_extra_hora, cidade, responsavel,
            telefone, email, instagram, escudo, escudo_blob,
            COALESCE(perfil_completo, FALSE) AS perfil_completo, cliente_id
        FROM equipes
        WHERE LOWER(TRIM(nome)) = LOWER(TRIM(%s))
          AND (%s::INTEGER IS NULL OR cliente_id = %s)
        ORDER BY id DESC
        LIMIT 1
    """

    def _executar(cnx):
        with cnx.cursor() as cur:
            cur.execute(sql, (nome, cid, cid))
            return cur.fetchone()

    if conn is not None:
        return _executar(conn)
    with conectar() as cnx:
        return _executar(cnx)


def colunas_equipes() -> set[str]:
    return buscar_colunas_tabela("equipes")
