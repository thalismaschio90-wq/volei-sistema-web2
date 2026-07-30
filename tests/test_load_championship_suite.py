from tests.load.config import LoadTestConfig
from tests.load.snapshots import build_snapshot_delta, extract_runtime_summary


def test_snapshot_delta_preserva_diferenca_numerica():
    before = {"despacho": {"bytes_emitidos": 100, "eventos": 2}}
    after = {"despacho": {"bytes_emitidos": 250, "eventos": 5}}
    delta = build_snapshot_delta(before, after)
    assert delta["despacho"]["bytes_emitidos"] == 150
    assert delta["despacho"]["eventos"] == 3


def test_runtime_summary_nao_expoe_urls():
    value = extract_runtime_summary({
        "ok": True,
        "runtime_config": {"workers": 2, "threads": 4, "database_url": "segredo"},
        "pool": {"conexoes_abertas": 3, "tempo_espera_total_ms": 12},
    })
    assert value["workers"] == 2
    assert value["pool"]["abertas"] == 3
    assert "database_url" not in value


def test_config_rejeita_mais_sockets_que_visualizadores():
    config = LoadTestConfig(
        base_url="http://localhost:5000",
        competicao="Teste",
        partida_id=1,
        viewers=5,
        socket_viewers=6,
        collect_admin_metrics=False,
    )
    assert any("SOCKET_VIEWERS" in error for error in config.validate())


def test_config_exige_cookie_para_metricas_admin():
    config = LoadTestConfig(
        base_url="http://localhost:5000",
        competicao="Teste",
        partida_id=1,
        viewers=5,
        socket_viewers=2,
        collect_admin_metrics=True,
    )
    assert any("SESSION_COOKIE" in error for error in config.validate())
