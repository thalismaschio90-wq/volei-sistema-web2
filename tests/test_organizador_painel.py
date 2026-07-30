from repositories.organizador_painel import buscar_painel_organizador
from services.organizador.painel import montar_painel_organizador


class Cursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.executions = []
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def execute(self, sql, params=()): self.executions.append((sql, params))
    def fetchone(self): return self.rows.pop(0) if self.rows else None
    def fetchall(self): return self.rows.pop(0) if self.rows else []


class Conn:
    def __init__(self, cursor): self._cursor = cursor
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def cursor(self): return self._cursor


def test_painel_organizador_usa_uma_conexao_e_consultas_consolidadas():
    cursor = Cursor([
        [{"nome": "Copa A", "organizador_login": "org"}],
        {"dados": True, "quadras": True, "estrutura": True, "regras": True, "classificacao": True, "avanco": False, "concluida": False},
        [{"id": 10, "equipe": "Equipe A", "tipo": "cadastro", "atleta_nome": None, "criado_em": None, "total_pendentes": 2}],
    ])
    chamadas = []
    dados = buscar_painel_organizador("org", conectar_fn=lambda: (chamadas.append(1) or Conn(cursor)))
    assert len(chamadas) == 1
    assert dados["competicao_atual"] == "Copa A"
    assert dados["status_config"]["concluida"] is True
    assert dados["solicitacoes_pendentes"] == 2
    assert len(cursor.executions) == 3


def test_painel_organizador_sem_competicao_nao_faz_consultas_auxiliares():
    cursor = Cursor([[]])
    dados = buscar_painel_organizador("org", conectar_fn=lambda: Conn(cursor))
    assert dados["competicao_atual"] == ""
    assert dados["solicitacoes_pendentes"] == 0
    assert len(cursor.executions) == 1
