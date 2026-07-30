from repositories.organizador_painel import buscar_painel_organizador
from repositories.superadmin_painel import buscar_painel_superadmin


class Cursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=()):
        self.executions.append((sql, params))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        return self.rows.pop(0) if self.rows else []


class Conn:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self._cursor

    def rollback(self):
        pass


def test_superadmin_inicial_nao_carrega_lista_completa_de_clientes():
    cursor = Cursor([
        {"login": "ThalisADM", "perfil": "superadmin", "ativo": True, "cliente_id": 1, "superadmin_nivel": "master"},
        {"total_competicoes": 4, "total_equipes": 12, "total_partidas": 30},
    ])
    dados = buscar_painel_superadmin("ThalisADM", conectar_fn=lambda: Conn(cursor))

    assert dados["superadmins_clientes"] == []
    assert len(cursor.executions) == 2
    assert all("u.senha" not in sql for sql, _ in cursor.executions)


def test_organizador_inicial_usa_colunas_especificas_e_tres_consultas():
    cursor = Cursor([
        [{"nome": "Copa A"}],
        {"dados": True, "quadras": True, "estrutura": True, "regras": True, "classificacao": True, "avanco": False, "concluida": False},
        [{"id": 10, "equipe": "Equipe A", "tipo": "novo_atleta", "atleta_nome": "José", "criado_em": None, "total_pendentes": 7}],
    ])
    dados = buscar_painel_organizador("org", conectar_fn=lambda: Conn(cursor))

    assert dados["solicitacoes_pendentes"] == 7
    assert dados["ultimas_solicitacoes"][0]["equipe"] == "Equipe A"
    assert "total_pendentes" not in dados["ultimas_solicitacoes"][0]
    assert dados["notificacoes_organizador"] == []
    assert len(cursor.executions) == 3
    assert all("SELECT *" not in sql.upper() for sql, _ in cursor.executions)
