from __future__ import annotations

import os

from core.sql_performance import (
    ConexaoInstrumentada,
    finalizar_medicao_sql,
    fingerprint_sql,
    iniciar_medicao_sql,
    instrumentar_conexao,
)


class CursorFake:
    def __init__(self):
        self.executadas = []

    def execute(self, query, params=None):
        self.executadas.append((query, params))
        return self

    def executemany(self, query, params_seq):
        self.executadas.append((query, list(params_seq)))
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class ConexaoFake:
    def __init__(self):
        self.cursor_fake = CursorFake()

    def cursor(self, *args, **kwargs):
        return self.cursor_fake


def test_fingerprint_nao_expoe_literais():
    a, op_a = fingerprint_sql("SELECT * FROM usuarios WHERE email='andre@teste.com' AND id=123")
    b, op_b = fingerprint_sql("SELECT * FROM usuarios WHERE email='outro@teste.com' AND id=999")
    assert a == b
    assert op_a == op_b == "SELECT"


def test_conexao_instrumentada_conta_execute(monkeypatch):
    monkeypatch.setenv("SQL_PERFORMANCE_LOG_ENABLED", "1")
    monkeypatch.setenv("SQL_SLOW_QUERY_THRESHOLD_MS", "999999")
    iniciar_medicao_sql()
    conexao = instrumentar_conexao(ConexaoFake())
    assert isinstance(conexao, ConexaoInstrumentada)
    with conexao.cursor() as cur:
        cur.execute("SELECT 1")
        cur.executemany("INSERT INTO x VALUES (%s)", [(1,), (2,)])
    stats = finalizar_medicao_sql()
    assert stats["quantidade"] == 2
    assert stats["duracao_total_ms"] >= 0


def test_instrumentacao_desabilitada_mantem_proxy_de_seguranca(monkeypatch):
    monkeypatch.setenv("SQL_PERFORMANCE_LOG_ENABLED", "0")
    original = ConexaoFake()
    conexao = instrumentar_conexao(original)
    assert isinstance(conexao, ConexaoInstrumentada)
    assert conexao._conexao is original
