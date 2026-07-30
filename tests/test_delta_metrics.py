from realtime.delta_metrics import DeltaMetricsStore


def test_delta_metrics_calcula_economia_e_saude(monkeypatch):
    monkeypatch.setenv("SOCKET_DELTA_HEALTH_MIN_APPLIED", "4")
    monkeypatch.setenv("SOCKET_DELTA_HEALTH_MAX_GAP_PERCENT", "5")
    monkeypatch.setenv(
        "SOCKET_DELTA_HEALTH_REQUIRED_CLIENTS",
        "apontador,arbitro,placar_profissional,visualizador_publico",
    )
    store = DeltaMetricsStore()
    store.registrar_delta_servidor(
        emitido=True,
        bytes_delta=200,
        bytes_estado=1000,
        economia_percentual=80,
    )
    for tipo in ["apontador", "arbitro_primeiro", "placar_profissional", "visualizador_publico"]:
        assert store.registrar_cliente(tipo, "delta_aplicado")
    dados = store.snapshot()
    assert dados["servidor"]["bytes_economizados"] == 800
    assert dados["servidor"]["economia_total_percentual"] == 80.0
    assert dados["saude"]["homologado"] is True
    assert "arbitro" in dados["saude"]["tipos_clientes_vistos"]


def test_delta_metrics_reprova_quando_ha_lacunas(monkeypatch):
    monkeypatch.setenv("SOCKET_DELTA_HEALTH_MIN_APPLIED", "1")
    monkeypatch.setenv("SOCKET_DELTA_HEALTH_MAX_GAP_PERCENT", "1")
    monkeypatch.setenv("SOCKET_DELTA_HEALTH_REQUIRED_CLIENTS", "apontador")
    store = DeltaMetricsStore()
    store.registrar_cliente("apontador", "delta_aplicado", 10)
    store.registrar_cliente("apontador", "lacuna_versao", 2)
    assert store.snapshot()["saude"]["homologado"] is False


def test_delta_metrics_rejeita_evento_desconhecido():
    store = DeltaMetricsStore()
    assert store.registrar_cliente("apontador", "conteudo_arbitrario") is False
    assert store.snapshot()["clientes"] == {}


def test_delta_metrics_limita_origem(monkeypatch):
    monkeypatch.setenv("SOCKET_DELTA_TELEMETRY_MAX_PER_MINUTE", "2")
    store = DeltaMetricsStore()
    assert store.permitir_origem("127.0.0.1") is True
    assert store.permitir_origem("127.0.0.1") is True
    assert store.permitir_origem("127.0.0.1") is False
