"""Verificações leves de schema usadas durante a execução normal.

Este módulo nunca cria ou altera estruturas. Quando uma migração obrigatória
não foi aplicada, gera uma mensagem explícita para o log/deploy.
"""
from __future__ import annotations

from repositories.conexao import conectar


class MigrationRequiredError(RuntimeError):
    """Indica que o banco precisa receber as migrações antes do servidor."""


def require_schema(*, tables=(), columns=None, context="operação") -> None:
    columns = columns or {}
    missing: list[str] = []
    with conectar() as conn:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema='public' AND table_name=%s LIMIT 1",
                    (table,),
                )
                if cur.fetchone() is None:
                    missing.append(f"tabela {table}")
            for table, required in columns.items():
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name=%s",
                    (table,),
                )
                found = {
                    row.get("column_name") if isinstance(row, dict) else row[0]
                    for row in (cur.fetchall() or [])
                }
                for column in required:
                    if column not in found:
                        missing.append(f"coluna {table}.{column}")
    if missing:
        detail = ", ".join(missing)
        raise MigrationRequiredError(
            f"Migração de banco pendente para {context}: {detail}. "
            "Execute 'python scripts/executar_migracoes.py' antes de iniciar o servidor."
        )
