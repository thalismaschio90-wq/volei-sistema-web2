from realtime.event_priority import (
    PRIORIDADE_BAIXA,
    PRIORIDADE_CRITICA,
    PRIORIDADE_NORMAL,
    RealtimeEventDispatcher,
    classificar_evento,
    dispatch_metrics_store,
)


class SocketFake:
    def __init__(self):
        self.emissoes = []
        self.tasks = []

    def emit(self, evento, payload, room=None, **kwargs):
        self.emissoes.append((evento, payload, room, kwargs))

    def start_background_task(self, fn, *args):
        self.tasks.append((fn, args))

    def sleep(self, _segundos):
        return None


def test_classifica_eventos_criticos_e_baixos():
    assert classificar_evento("placar_rapido") == PRIORIDADE_CRITICA
    assert classificar_evento("telemetria_realtime") == PRIORIDADE_BAIXA
    assert classificar_evento("ultima_acao_arbitros") == PRIORIDADE_NORMAL


def test_evento_critico_e_emitido_imediatamente(monkeypatch):
    monkeypatch.setenv("SOCKET_PRIORITY_ENABLED", "1")
    dispatcher = RealtimeEventDispatcher()
    socket = SocketFake()

    assert dispatcher.publicar(socket, "placar_rapido", {"pontos_a": 1}, sala="partida:10")
    assert socket.emissoes == [("placar_rapido", {"pontos_a": 1}, "partida:10", {})]


def test_evento_baixo_e_agrupado_mantendo_o_mais_recente(monkeypatch):
    monkeypatch.setenv("SOCKET_PRIORITY_ENABLED", "1")
    dispatcher = RealtimeEventDispatcher()
    socket = SocketFake()

    dispatcher.publicar(socket, "telemetria_realtime", {"valor": 1}, sala="partida:10")
    dispatcher.publicar(socket, "telemetria_realtime", {"valor": 2}, sala="partida:10")

    assert socket.emissoes == []
    assert dispatcher.flush() == 1
    assert socket.emissoes[0][:3] == ("telemetria_realtime", {"valor": 2}, "partida:10")


def test_duplicata_exata_pode_ser_descartada(monkeypatch):
    monkeypatch.setenv("SOCKET_PRIORITY_ENABLED", "1")
    dispatcher = RealtimeEventDispatcher()
    socket = SocketFake()
    payload = {"pontos_a": 5, "pontos_b": 4}

    assert dispatcher.publicar(
        socket,
        "placar_rapido",
        payload,
        sala="partida:10",
        deduplicar_ms=100,
    )
    assert not dispatcher.publicar(
        socket,
        "placar_rapido",
        payload,
        sala="partida:10",
        deduplicar_ms=100,
    )
    assert len(socket.emissoes) == 1


def test_metricas_de_despacho_nao_guardam_payload(monkeypatch):
    monkeypatch.setenv("SOCKET_PRIORITY_ENABLED", "1")
    dispatch_metrics_store.limpar()
    dispatcher = RealtimeEventDispatcher()
    socket = SocketFake()

    dispatcher.publicar(socket, "placar_rapido", {"segredo": "nao guardar"}, sala="x")
    dados = dispatch_metrics_store.snapshot()

    assert dados["emitidos_critica"] >= 1
    assert "segredo" not in repr(dados)


def test_metricas_registram_bytes_e_economia_por_duplicidade(monkeypatch):
    monkeypatch.setenv("SOCKET_PRIORITY_ENABLED", "1")
    dispatch_metrics_store.limpar()
    dispatcher = RealtimeEventDispatcher()
    socket = SocketFake()
    payload = {"pontos_a": 12, "pontos_b": 11}

    dispatcher.publicar(socket, "placar_rapido", payload, sala="partida:20", deduplicar_ms=1000)
    dispatcher.publicar(socket, "placar_rapido", payload, sala="partida:20", deduplicar_ms=1000)
    dados = dispatch_metrics_store.snapshot()

    assert dados["bytes_emitidos_estimados"] > 0
    assert dados["bytes_economizados_despacho"] > 0
    assert dados["economia_despacho_percentual"] > 0
    assert dados["eventos_por_trafego"][0]["evento"] == "placar_rapido"


def test_agrupamento_conta_payload_substituido_como_economia(monkeypatch):
    monkeypatch.setenv("SOCKET_PRIORITY_ENABLED", "1")
    dispatch_metrics_store.limpar()
    dispatcher = RealtimeEventDispatcher()
    socket = SocketFake()

    dispatcher.publicar(socket, "telemetria_realtime", {"valor": "primeiro"}, sala="partida:30")
    dispatcher.publicar(socket, "telemetria_realtime", {"valor": "segundo"}, sala="partida:30")
    dispatcher.flush()
    dados = dispatch_metrics_store.snapshot()

    assert dados["agrupados"] == 1
    assert dados["bytes_economizados_despacho"] > 0
    assert dados["emitidos_baixa"] == 1
