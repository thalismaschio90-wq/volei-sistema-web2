"""Helpers padronizados para consultas e transações curtas."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from .conexao import conectar


@contextmanager
def somente_leitura() -> Iterator[Any]:
    """Entrega uma conexão para leitura, com rollback defensivo ao finalizar."""
    with conectar() as conn:
        try:
            yield conn
        finally:
            # SELECT também pode abrir transação no PostgreSQL; o rollback curto
            # impede conexões "idle in transaction" ao retornar ao pool.
            try:
                conn.rollback()
            except Exception:
                pass


@contextmanager
def transacao() -> Iterator[Any]:
    """Garante atomicidade: commit no sucesso e rollback em qualquer erro."""
    with conectar() as conn:
        try:
            yield conn
            conn.commit()
        except BaseException:
            try:
                conn.rollback()
            finally:
                raise
