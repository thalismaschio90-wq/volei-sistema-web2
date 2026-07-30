from __future__ import annotations

from contextlib import contextmanager

import repositories.conexao as conexao


class ConnFake:
    def __init__(self, *, ping_ok=True):
        self.closed = False
        self.broken = False
        self.ping_ok = ping_ok

    @contextmanager
    def cursor(self):
        conn = self

        class Cursor:
            def execute(self, sql):
                if not conn.ping_ok:
                    raise RuntimeError("connection is closed")
                return self

            def fetchone(self):
                return (1,)

        yield Cursor()

    def close(self):
        self.closed = True


class PoolContextFake:
    def __init__(self, conn):
        self.conn = conn
        self.exit_calls = []

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        self.exit_calls.append((exc_type, exc))
        return False


class PoolFake:
    def __init__(self, conn):
        self.context = PoolContextFake(conn)

    def connection(self, timeout=None):
        return self.context


def _reset(monkeypatch):
    monkeypatch.setattr(conexao, "_DB_POOL", None)
    monkeypatch.setattr(conexao, "_DIRECT_FALLBACK_SEMAPHORE", None)
    monkeypatch.setattr(conexao, "_DIRECT_FALLBACK_LIMIT", None)
    for chave in list(conexao._METRICS):
        conexao._METRICS[chave] = 0


def test_ping_invalido_descarta_so_conexao_sem_fechar_pool(monkeypatch):
    _reset(monkeypatch)
    ruim = ConnFake(ping_ok=False)
    pool = PoolFake(ruim)
    direta = ConnFake(ping_ok=True)
    fechamentos_pool = []

    monkeypatch.setattr(conexao, "_obter_pool", lambda: pool)
    monkeypatch.setattr(conexao, "_conexao_direta", lambda: direta)
    monkeypatch.setattr(conexao, "fechar_pool", lambda timeout=1: fechamentos_pool.append(timeout))
    monkeypatch.setenv("DB_DIRECT_FALLBACK_MAX", "1")

    with conexao.conectar() as conn:
        assert conn._conexao is direta

    assert ruim.closed is True
    assert fechamentos_pool == []
    assert conexao.obter_estatisticas_pool()["pool_conexoes_descartadas"] == 1


def test_metricas_contabilizam_conexao_ativa(monkeypatch):
    _reset(monkeypatch)
    boa = ConnFake(ping_ok=True)
    pool = PoolFake(boa)
    monkeypatch.setattr(conexao, "_obter_pool", lambda: pool)

    with conexao.conectar():
        durante = conexao.obter_estatisticas_pool()
        assert durante["pool_ativas"] == 1
        assert durante["pool_ativas_max"] == 1

    depois = conexao.obter_estatisticas_pool()
    assert depois["pool_ativas"] == 0
