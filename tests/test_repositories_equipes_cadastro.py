from __future__ import annotations

from repositories import equipes_cadastro


class FakeCursor:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
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

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


def instalar_dependencias(monkeypatch, conn, **overrides):
    dependencias = {
        "conectar": lambda: conn,
        "cliente_id_por_competicao": lambda *args, **kwargs: 7,
        "normalizar_login_equipe": lambda _: "equipe_azul",
        "gerar_login_unico": lambda login, **kwargs: login,
        "gerar_senha_aleatoria": lambda _: "Senha123",
        "buscar_equipe_global_por_nome": lambda *args, **kwargs: None,
        "gerar_hash_senha": lambda senha: f"hash:{senha}",
    }
    dependencias.update(overrides)
    for nome, valor in dependencias.items():
        monkeypatch.setattr(equipes_cadastro, nome, valor)


def equipe_row():
    return {
        "nome": "Equipe Azul",
        "login": "equipe_azul",
        "senha": "Senha123",
        "competicao": "Copa",
        "cidade": "",
        "responsavel": "",
        "telefone": "",
        "email": "",
        "instagram": "",
        "escudo": "/static/img/escudo_padrao.svg",
        "escudo_blob": None,
        "escudo_exibicao": "/static/img/escudo_padrao.svg",
        "perfil_completo": False,
        "cliente_id": 7,
    }


def test_vincular_existente_preserva_credenciais(monkeypatch):
    cursor = FakeCursor(rows=[equipe_row(), None])
    conn = FakeConn(cursor)
    instalar_dependencias(monkeypatch, conn)

    resultado = equipes_cadastro.vincular_equipe_existente_competicao_persistencia(
        "equipe_azul", "Copa"
    )

    assert resultado["login"] == "equipe_azul"
    assert resultado["senha"] == "Senha123"
    assert resultado["ja_vinculada"] is False
    assert conn.commits == 1
    assert any("INSERT INTO equipes_competicoes" in sql for sql, _ in cursor.executed)


def test_vincular_existente_indica_vinculo_preexistente(monkeypatch):
    cursor = FakeCursor(rows=[equipe_row(), {"id": 4}])
    conn = FakeConn(cursor)
    instalar_dependencias(monkeypatch, conn)

    resultado = equipes_cadastro.vincular_equipe_existente_competicao_persistencia(
        "equipe_azul", "Copa"
    )

    assert resultado["ja_vinculada"] is True


def test_criar_nova_equipe_grava_tres_registros(monkeypatch):
    cursor = FakeCursor()
    conn = FakeConn(cursor)
    instalar_dependencias(monkeypatch, conn)

    resultado = equipes_cadastro.criar_nova_equipe_com_credenciais_persistencia(
        "  Equipe   Azul ", "Copa"
    )

    assert resultado == {
        "login": "equipe_azul",
        "senha": "Senha123",
        "nome": "Equipe Azul",
        "vinculada": True,
        "ja_existia": False,
        "ja_vinculada": False,
    }
    assert conn.commits == 1
    assert len(cursor.executed) == 3
    assert "INSERT INTO equipes" in cursor.executed[0][0]
    assert "INSERT INTO equipes_competicoes" in cursor.executed[1][0]
    assert "INSERT INTO usuarios" in cursor.executed[2][0]


def test_criar_ou_vincular_reutiliza_equipe_existente(monkeypatch):
    cursor = FakeCursor(rows=[equipe_row(), None])
    conn = FakeConn(cursor)
    existente = equipe_row()
    instalar_dependencias(
        monkeypatch,
        conn,
        buscar_equipe_global_por_nome=lambda *args, **kwargs: existente,
    )

    resultado = equipes_cadastro.criar_equipe_com_credenciais_persistencia(
        "Equipe Azul", "Copa"
    )

    assert resultado["ja_existia"] is True
    assert resultado["login"] == "equipe_azul"
    assert conn.commits == 1


def test_rejeita_dados_vazios(monkeypatch):
    conn = FakeConn(FakeCursor())
    instalar_dependencias(monkeypatch, conn)
    assert equipes_cadastro.criar_nova_equipe_com_credenciais_persistencia("", "Copa") is None
    assert equipes_cadastro.vincular_equipe_existente_competicao_persistencia("", "Copa") is None
