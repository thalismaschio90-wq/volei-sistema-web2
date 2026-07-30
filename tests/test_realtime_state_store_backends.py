from __future__ import annotations

import importlib

from realtime.state_store import LocalEstadoPartidaStore, RedisEstadoPartidaStore


def test_store_local_mantem_copia_e_versao():
    store = LocalEstadoPartidaStore()
    salvo = store.salvar(10, {"pontos_a": 1, "rotacao_a": [1, 2, 3]})
    assert salvo is not None
    assert salvo.versao == 1

    recebido = store.obter(10)
    assert recebido is not None
    recebido["rotacao_a"][0] = 99
    assert store.obter(10)["rotacao_a"][0] == 1


def test_redis_decodifica_payload_com_metadados():
    payload = '{"estado":{"pontos_a":7},"versao":12,"atualizado_em":123.5}'
    item = RedisEstadoPartidaStore._decodificar(payload)
    assert item is not None
    assert item.versao == 12
    assert item.estado["estado_versao"] == 12
    assert item.estado["pontos_a"] == 7


def test_factory_padrao_permanece_local(monkeypatch):
    monkeypatch.delenv("REALTIME_STATE_BACKEND", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    modulo = importlib.import_module("realtime.state_store")
    store = modulo.criar_estado_partidas_store()
    assert isinstance(store, LocalEstadoPartidaStore)


def test_factory_redis_sem_url_faz_fallback_local(monkeypatch):
    monkeypatch.setenv("REALTIME_STATE_BACKEND", "redis")
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("REALTIME_REDIS_URL", raising=False)
    monkeypatch.delenv("REALTIME_REDIS_REQUIRED", raising=False)
    modulo = importlib.import_module("realtime.state_store")
    store = modulo.criar_estado_partidas_store()
    assert isinstance(store, LocalEstadoPartidaStore)


def test_factory_auto_sem_url_permanece_local(monkeypatch):
    monkeypatch.setenv("REALTIME_STATE_BACKEND", "auto")
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("REALTIME_REDIS_URL", raising=False)
    modulo = importlib.import_module("realtime.state_store")
    store = modulo.criar_estado_partidas_store()
    assert isinstance(store, LocalEstadoPartidaStore)


def test_factory_auto_com_url_tenta_redis(monkeypatch):
    monkeypatch.setenv("REALTIME_STATE_BACKEND", "auto")
    monkeypatch.setenv("REDIS_URL", "redis://fake:6379/0")
    monkeypatch.setenv("REALTIME_REDIS_REQUIRED", "1")
    modulo = importlib.import_module("realtime.state_store")

    class FakeRedisStore:
        backend = "redis"
        def __init__(self, redis_url, **kwargs):
            assert redis_url == "redis://fake:6379/0"
        def testar_conexao(self):
            return True

    monkeypatch.setattr(modulo, "RedisEstadoPartidaStore", FakeRedisStore)
    store = modulo.criar_estado_partidas_store()
    assert isinstance(store, FakeRedisStore)
