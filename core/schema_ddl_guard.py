"""Proteção contra alterações de schema durante a execução normal.

O VolleyTablePro permite DDL somente dentro do executor versionado de
migrações. A proteção fica no cursor central de banco, portanto também cobre
rotinas legadas que ainda contenham SQL estrutural por compatibilidade.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import re
from typing import Any, Iterator

_DDL_LIBERADO: ContextVar[bool] = ContextVar("vtp_schema_ddl_liberado", default=False)

# Remove comentários iniciais antes de identificar a primeira instrução.
_COMENTARIO_INICIAL_RE = re.compile(
    r"^(?:\s|--[^\n]*(?:\n|$)|/\*.*?\*/)*",
    flags=re.DOTALL,
)
_DDL_RE = re.compile(
    r"^(?:CREATE|ALTER|DROP|TRUNCATE|COMMENT|GRANT|REVOKE|REINDEX|CLUSTER|VACUUM)\b",
    flags=re.IGNORECASE,
)


class DDLForaDeMigracaoError(RuntimeError):
    """Indica tentativa de alterar o schema fora de uma migração."""


def ddl_liberado() -> bool:
    return bool(_DDL_LIBERADO.get())


def operacao_ddl(sql: Any) -> bool:
    texto = str(sql or "")
    texto = _COMENTARIO_INICIAL_RE.sub("", texto, count=1).lstrip()
    return bool(_DDL_RE.match(texto))


def validar_sql_sem_ddl(sql: Any) -> None:
    if operacao_ddl(sql) and not ddl_liberado():
        raise DDLForaDeMigracaoError(
            "Alteração de schema bloqueada durante a execução normal. "
            "Execute as migrações versionadas antes de iniciar o aplicativo."
        )


@contextmanager
def permitir_ddl_migracao() -> Iterator[None]:
    """Libera DDL apenas no contexto controlado do executor de migrações."""
    token = _DDL_LIBERADO.set(True)
    try:
        yield
    finally:
        _DDL_LIBERADO.reset(token)
