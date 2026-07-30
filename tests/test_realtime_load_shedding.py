import os

from realtime.load_shedding import (
    MODO_CONTROLADO,
    MODO_CRITICO,
    MODO_NORMAL,
    RealtimeLoadSheddingManager,
)


def test_modo_normal_por_padrao(monkeypatch):
    monkeypatch.delenv("SOCKET_DEGRADATION_FORCE_MODE", raising=False)
    monkeypatch.setenv("SOCKET_DEGRADATION_ENABLED", "1")
    gerenciador = RealtimeLoadSheddingManager()
    assert gerenciador.observar_evento(tamanho_fila=0) == MODO_NORMAL


def test_modo_controlado_por_fila(monkeypatch):
    monkeypatch.setenv("SOCKET_DEGRADATION_CONTROLLED_QUEUE", "2")
    monkeypatch.setenv("SOCKET_DEGRADATION_CRITICAL_QUEUE", "10")
    gerenciador = RealtimeLoadSheddingManager()
    assert gerenciador.observar_evento(tamanho_fila=2) == MODO_CONTROLADO
    assert gerenciador.atraso_baixa_ms(100) >= 200


def test_modo_critico_descarta_apenas_baixa(monkeypatch):
    monkeypatch.setenv("SOCKET_DEGRADATION_FORCE_MODE", "critico")
    monkeypatch.setenv("SOCKET_DEGRADATION_DROP_LOW_ON_CRITICAL", "1")
    gerenciador = RealtimeLoadSheddingManager()
    gerenciador.observar_evento(tamanho_fila=0)
    assert gerenciador.modo_atual() == MODO_CRITICO
    assert gerenciador.deve_descartar_baixa() is True
    gerenciador.registrar_descarte_baixa()
    assert gerenciador.snapshot()["eventos_baixa_descartados"] == 1
    assert gerenciador.snapshot()["eventos_criticos_protegidos"] is True


def test_desabilitado_permanece_normal(monkeypatch):
    monkeypatch.delenv("SOCKET_DEGRADATION_FORCE_MODE", raising=False)
    monkeypatch.setenv("SOCKET_DEGRADATION_ENABLED", "0")
    monkeypatch.setenv("SOCKET_DEGRADATION_CONTROLLED_QUEUE", "1")
    gerenciador = RealtimeLoadSheddingManager()
    assert gerenciador.observar_evento(tamanho_fila=999) == MODO_NORMAL
