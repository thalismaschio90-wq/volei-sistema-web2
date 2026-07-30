from realtime.delta_metrics import DeltaMetricsStore


def test_registra_renderizacao_agregada():
    store = DeltaMetricsStore()
    assert store.registrar_render_cliente("apontador", 12.5, 3)
    assert store.registrar_render_cliente("apontador", 7.5, 1)
    dados = store.snapshot()["renderizacao"]["apontador"]
    assert dados["renderizacoes"] == 2
    assert dados["atualizacoes_agregadas"] == 4
    assert dados["duracao_media_ms"] == 10.0
    assert dados["duracao_max_ms"] == 12.5


def test_rejeita_renderizacao_invalida():
    store = DeltaMetricsStore()
    assert not store.registrar_render_cliente("apontador", "invalido", 1)
    assert not store.registrar_render_cliente("apontador", -1, 1)
    assert not store.registrar_render_cliente("apontador", 60001, 1)
    assert store.snapshot()["renderizacao"] == {}


def test_agrupa_tipos_de_arbitro():
    store = DeltaMetricsStore()
    assert store.registrar_render_cliente("arbitro_primeiro", 5, 1)
    assert store.registrar_render_cliente("arbitro_segundo", 7, 1)
    dados = store.snapshot()["renderizacao"]["arbitro"]
    assert dados["renderizacoes"] == 2
    assert dados["duracao_media_ms"] == 6.0
