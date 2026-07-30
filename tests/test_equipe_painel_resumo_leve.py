from contextlib import contextmanager

from repositories import atletas as atletas_repo
from services.equipes.painel import montar_resumo_painel


class _Cursor:
    def __init__(self):
        self.sql = ""
        self.params = None

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchone(self):
        return {"total": 12, "aprovados": 10, "pendentes": 1, "reprovados": 1}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor



def test_resumo_atletas_usa_agregacao_sem_select_all(monkeypatch):
    cursor = _Cursor()

    @contextmanager
    def conectar_fake():
        yield _Conn(cursor)

    monkeypatch.setattr(atletas_repo, "conectar", conectar_fake)

    resumo = atletas_repo.resumir_atletas_da_equipe("Equipe A", "Copa")

    assert resumo == {"total": 12, "aprovados": 10, "pendentes": 1, "reprovados": 1}
    assert "SELECT *" not in cursor.sql.upper()
    assert "COUNT(*) FILTER" in cursor.sql.upper()
    assert cursor.params == ("Equipe A", "Copa")


def test_painel_aceita_contadores_sem_lista_completa_de_atletas():
    resumo = montar_resumo_painel(
        {"total": 12, "aprovados": 12, "pendentes": 0, "reprovados": 0},
        [],
        {"limite_atletas": 12},
    )

    assert resumo["total_atletas"] == 12
    assert resumo["status_equipe"] == "Equipe completa"
    assert resumo["status_classe"] == "tag-aprovado"
