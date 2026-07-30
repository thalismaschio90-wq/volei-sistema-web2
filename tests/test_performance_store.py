from core.performance_store import PerformanceStore


def test_store_agrega_rotas_e_consultas_lentas(monkeypatch):
    monkeypatch.setenv("PERFORMANCE_SAMPLE_LIMIT", "50")
    store = PerformanceStore()
    sql = {
        "quantidade": 3,
        "duracao_total_ms": 90,
        "duracao_max_ms": 60,
        "lentas": [{"fingerprint": "abc123", "operacao": "SELECT", "duracao_ms": 60}],
    }
    store.registrar_requisicao(metodo="GET", endpoint="painel.inicio", rota="/inicio", status=200, duracao_ms=120, sql=sql)
    store.registrar_requisicao(metodo="GET", endpoint="painel.inicio", rota="/inicio", status=500, duracao_ms=240, sql=sql)
    snap = store.snapshot()
    rota = snap["rotas"][0]
    assert rota["quantidade"] == 2
    assert rota["erros"] == 1
    assert rota["duracao_media_ms"] == 180
    assert rota["sql_media_consultas"] == 3
    consulta = snap["consultas_lentas"][0]
    assert consulta["fingerprint"] == "abc123"
    assert consulta["quantidade"] == 2


def test_store_limpar_remove_metricas():
    store = PerformanceStore()
    store.registrar_requisicao(metodo="GET", endpoint="x", rota="/x", status=200, duracao_ms=1, sql={})
    store.limpar()
    snap = store.snapshot()
    assert snap["rotas"] == []
    assert snap["consultas_lentas"] == []


def test_store_expoe_diagnostico_estrutural_da_consulta():
    store = PerformanceStore()
    sql = {
        "quantidade": 1,
        "duracao_total_ms": 500,
        "duracao_max_ms": 500,
        "lentas": [{
            "fingerprint": "slow123",
            "operacao": "SELECT",
            "duracao_ms": 500,
            "estrutura": {
                "operacao": "SELECT",
                "tabelas": ["partidas"],
                "filtros": ["competicao", "status"],
                "ordenacao": ["rodada"],
                "agrupamento": [],
                "tem_join": False,
                "tem_select_star": False,
            },
        }],
    }
    store.registrar_requisicao(metodo="GET", endpoint="x", rota="/x", status=200, duracao_ms=510, sql=sql)
    consulta = store.snapshot()["consultas_lentas"][0]
    assert consulta["estrutura"]["tabelas"] == ["partidas"]
    assert any(item["tipo"] == "indice" for item in consulta["sugestoes"])
