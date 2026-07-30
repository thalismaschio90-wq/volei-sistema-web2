from repositories.superadmin_painel import buscar_painel_superadmin


class Cursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.executions = []
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def execute(self, sql, params=()):
        self.executions.append((sql, params))
    def fetchone(self):
        return self.rows.pop(0) if self.rows else None
    def fetchall(self):
        return self.rows.pop(0) if self.rows else []


class Conn:
    def __init__(self, cursor): self._cursor = cursor
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def cursor(self): return self._cursor
    def rollback(self): pass


def test_painel_master_usa_uma_conexao_e_retorna_totais_clientes():
    cursor = Cursor([
        {"login": "ThalisADM", "perfil": "superadmin", "ativo": True, "cliente_id": 1, "superadmin_nivel": "master"},
        {"total_competicoes": 4, "total_equipes": 12, "total_partidas": 30},
    ])
    chamadas = []
    def conectar():
        chamadas.append(1)
        return Conn(cursor)

    dados = buscar_painel_superadmin("ThalisADM", conectar_fn=conectar)
    assert len(chamadas) == 1
    assert dados["eh_master"] is True
    assert dados["total_partidas"] == 30
    assert dados["superadmins_clientes"] == []
    assert len(cursor.executions) == 2


def test_painel_cliente_nao_lista_outros_superadmins():
    cursor = Cursor([
        {"login": "adm_a", "perfil": "superadmin", "ativo": True, "cliente_id": 9, "superadmin_nivel": "cliente"},
        {"total_competicoes": 2, "total_equipes": 5, "total_partidas": 8},
    ])
    dados = buscar_painel_superadmin("adm_a", conectar_fn=lambda: Conn(cursor))
    assert dados["eh_master"] is False
    assert dados["superadmins_clientes"] == []
    assert len(cursor.executions) == 2
