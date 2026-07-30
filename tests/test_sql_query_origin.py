from __future__ import annotations

from core import sql_performance
from core.performance_store import PerformanceStore


def test_origem_consulta_nao_expoe_dados(monkeypatch):
    monkeypatch.setenv("SQL_PERFORMANCE_LOG_ENABLED", "1")
    monkeypatch.setenv("SQL_SLOW_QUERY_THRESHOLD_MS", "0")
    sql_performance.iniciar_medicao_sql()
    sql_performance.registrar_consulta(
        "SELECT * FROM usuarios WHERE cpf = %s AND senha = %s",
        12.5,
    )
    dados = sql_performance.finalizar_medicao_sql()
    item = dados["lentas"][0]
    assert "origem" in item
    assert "123" not in item["origem"]
    assert "senha" not in item["origem"].lower()


def test_store_agrega_origens_sem_duplicar():
    store = PerformanceStore()
    sql = {
        "quantidade": 1,
        "duracao_total_ms": 400,
        "duracao_max_ms": 400,
        "lentas": [
            {
                "fingerprint": "abc123",
                "operacao": "SELECT",
                "duracao_ms": 400,
                "origem": "repositories/partidas.py:listar:88",
            }
        ],
    }
    for _ in range(2):
        store.registrar_requisicao(
            metodo="GET",
            endpoint="painel",
            rota="/inicio",
            status=200,
            duracao_ms=500,
            sql=sql,
        )
    consulta = store.snapshot()["consultas_lentas"][0]
    assert consulta["origens"] == ["repositories/partidas.py:listar:88"]
    assert consulta["quantidade"] == 2
