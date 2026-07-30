from scripts.aplicar_indices_criticos import INDICES, executar


def test_indices_possuem_nomes_unicos():
    nomes = [item.nome for item in INDICES]
    assert len(nomes) == len(set(nomes))


def test_indices_sao_concorrentes_e_idempotentes():
    for item in INDICES:
        sql = item.sql.upper()
        assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS" in sql
        assert item.nome in item.sql


def test_modo_simulacao_nao_abre_banco(monkeypatch):
    def falhar():
        raise AssertionError("não deveria conectar")

    monkeypatch.setattr("scripts.aplicar_indices_criticos.conectar", falhar)
    assert executar(aplicar=False) == 0
