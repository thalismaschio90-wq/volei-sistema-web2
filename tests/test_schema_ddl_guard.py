import pytest

from core.schema_ddl_guard import (
    DDLForaDeMigracaoError,
    operacao_ddl,
    permitir_ddl_migracao,
    validar_sql_sem_ddl,
)


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE exemplo (id INTEGER)",
        " ALTER TABLE exemplo ADD COLUMN nome TEXT",
        "-- comentário\nDROP INDEX idx_exemplo",
        "/* comentário */ TRUNCATE TABLE exemplo",
        "CREATE INDEX idx_exemplo ON exemplo (id)",
    ],
)
def test_bloqueia_ddl_fora_de_migracao(sql):
    assert operacao_ddl(sql)
    with pytest.raises(DDLForaDeMigracaoError):
        validar_sql_sem_ddl(sql)


def test_libera_ddl_somente_no_contexto_da_migracao():
    with permitir_ddl_migracao():
        validar_sql_sem_ddl("ALTER TABLE exemplo ADD COLUMN nome TEXT")

    with pytest.raises(DDLForaDeMigracaoError):
        validar_sql_sem_ddl("ALTER TABLE exemplo ADD COLUMN outro TEXT")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM exemplo",
        "INSERT INTO exemplo (id) VALUES (1)",
        "UPDATE exemplo SET id = 2",
        "DELETE FROM exemplo",
        "WITH dados AS (SELECT 1) SELECT * FROM dados",
    ],
)
def test_permite_dml_e_consultas(sql):
    assert not operacao_ddl(sql)
    validar_sql_sem_ddl(sql)
