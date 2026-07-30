"""Consultas de metadados de schema usadas apenas por compatibilidade.

Novos fluxos devem preferir schema versionado por migrações. Este módulo existe
para retirar dependências de repositórios em relação a ``banco.py`` enquanto os
caminhos legados são eliminados gradualmente.
"""
from __future__ import annotations

from functools import lru_cache

from repositories.conexao import conectar


@lru_cache(maxsize=128)
def buscar_colunas_tabela(nome_tabela: str) -> frozenset[str]:
    tabela = str(nome_tabela or "").strip()
    if not tabela:
        return frozenset()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                """,
                (tabela,),
            )
            return frozenset(row["column_name"] for row in (cur.fetchall() or []))


def invalidar_cache_colunas(nome_tabela: str | None = None) -> None:
    # O lru_cache não oferece invalidação por chave de forma pública. Como este
    # helper é temporário e usado apenas em compatibilidade, a limpeza global é
    # simples, barata e previsível.
    buscar_colunas_tabela.cache_clear()
