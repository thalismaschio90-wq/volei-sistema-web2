from __future__ import annotations

from repositories import equipes_escrita


class FakeCursor:
    def __init__(self, rows=None, rowcount=1):
        self.rows = list(rows or [])
        self.rowcount = rowcount
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def instalar_dependencias(monkeypatch, conn):
    monkeypatch.setattr(equipes_escrita, "conectar", lambda: conn)
    monkeypatch.setattr(equipes_escrita, "gerar_senha_aleatoria", lambda _: "Senha123")
    monkeypatch.setattr(equipes_escrita, "gerar_hash_senha", lambda senha: f"hash:{senha}")


def test_atualizar_quadro_tecnico(monkeypatch):
    cursor = FakeCursor()
    conn = FakeConn(cursor)
    instalar_dependencias(monkeypatch, conn)
    resultado = equipes_escrita.atualizar_quadro_tecnico_equipe_persistencia(
        "Equipe", "Copa", "T", "A", "P", "M"
    )
    assert resultado == (True, "Atualizado com sucesso!")
    assert conn.commits == 1
    assert cursor.executed[0][1][-2:] == ("Equipe", "Copa")


def test_redefinir_senha(monkeypatch):
    cursor = FakeCursor(rows=[{"login": "eq_teste"}])
    conn = FakeConn(cursor)
    instalar_dependencias(monkeypatch, conn)
    resultado = equipes_escrita.redefinir_senha_da_equipe_persistencia("Equipe", "Copa")
    assert resultado == {"login": "eq_teste", "senha": "Senha123"}
    assert len(cursor.executed) == 3
    assert conn.commits == 1


def test_excluir_equipe_remove_vinculo(monkeypatch):
    cursor = FakeCursor(rows=[
        {"travada": False},
        {
            "id": 9,
            "equipe_login": "eq_teste",
            "equipe_nome": "Equipe Antiga",
            "login_global": "eq_teste",
            "nome_global": "Equipe Nova",
        },
    ])
    conn = FakeConn(cursor)
    instalar_dependencias(monkeypatch, conn)
    assert equipes_escrita.excluir_equipe_persistencia("Equipe Nova", "Copa") is True
    assert conn.commits == 1
    assert any("DELETE FROM equipes_competicoes" in sql for sql, _ in cursor.executed)


def test_excluir_equipe_rejeita_dados_vazios(monkeypatch):
    conn = FakeConn(FakeCursor())
    instalar_dependencias(monkeypatch, conn)
    assert equipes_escrita.excluir_equipe_persistencia("", "Copa") is False
