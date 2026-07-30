from __future__ import annotations

from repositories import equipes_perfil


class Cursor:
    def __init__(self, rows=None, rowcounts=None):
        self.rows = list(rows or [])
        self.rowcounts = list(rowcounts or [1] * 50)
        self.rowcount = 0
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))
        self.rowcount = self.rowcounts.pop(0) if self.rowcounts else 1

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


class Conn:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


def instalar_dependencias(monkeypatch, conn):
    monkeypatch.setattr(equipes_perfil, "conectar", lambda: conn)
    monkeypatch.setattr(
        equipes_perfil,
        "colunas_equipes",
        lambda: {"escudo", "escudo_blob", "competicao", "equipe"},
    )
    monkeypatch.setattr(
        equipes_perfil,
        "buscar_colunas_tabela",
        lambda _: {"equipe", "competicao"},
    )


def test_salvar_perfil_por_login(monkeypatch):
    cursor = Cursor()
    conn = Conn(cursor)
    instalar_dependencias(monkeypatch, conn)
    assert equipes_perfil.salvar_perfil_equipe_por_login_persistencia(
        "eq", "Cidade", "Resp", "999"
    ) is True
    assert conn.commits == 1
    assert cursor.executed[0][1][-1] == "eq"


def test_atualizar_escudo_fallback_nome(monkeypatch):
    cursor = Cursor(rows=[{"equipe": "Equipe"}], rowcounts=[0, 1, 1])
    conn = Conn(cursor)
    instalar_dependencias(monkeypatch, conn)
    assert equipes_perfil.atualizar_escudo_equipe_por_login_persistencia("login", "data") is True
    assert any("SELECT equipe FROM usuarios" in sql for sql, _ in cursor.executed)


def test_perfil_incompleto(monkeypatch):
    cursor = Cursor(rows=[{"cidade": "X", "responsavel": "Y", "telefone": ""}])
    conn = Conn(cursor)
    instalar_dependencias(monkeypatch, conn)
    assert equipes_perfil.perfil_equipe_incompleto_por_login_consulta("eq", conn=conn) is True


def test_renomear_atualiza_vinculos(monkeypatch):
    cursor = Cursor(rows=[
        {"travada": False},
        {"login": "eq", "nome_global": "Antiga", "nome_vinculo": "Antiga"},
    ])
    conn = Conn(cursor)
    instalar_dependencias(monkeypatch, conn)
    resultado = equipes_perfil.atualizar_nome_equipe_persistencia("Antiga", "Copa", "Nova")
    assert resultado == (True, "Atualizado com sucesso!")
    assert conn.commits == 1
    assert any("UPDATE partidas" in sql for sql, _ in cursor.executed)
