import os

from services.relatorios import fila
from tasks.relatorios import SolicitacaoRelatorio


def test_fila_desabilitada_sem_variavel(monkeypatch):
    monkeypatch.delenv("RELATORIOS_ASYNC_ENABLED", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    assert fila.fila_habilitada() is False


def test_fila_habilitada_com_redis(monkeypatch):
    monkeypatch.setenv("RELATORIOS_ASYNC_ENABLED", "1")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    assert fila.fila_habilitada() is True


def test_solicitacao_serializavel():
    s = SolicitacaoRelatorio(tipo="ranking_atletas", competicao="Copa")
    assert s.serializar()["tipo"] == "ranking_atletas"
