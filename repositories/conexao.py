"""Conexões PostgreSQL/Neon centralizadas.

Este módulo é a única implementação do pool de conexões. ``banco.py`` mantém
apenas aliases de compatibilidade, permitindo migrar os repositórios por domínio
sem alterar todas as rotas de uma vez.
"""
from __future__ import annotations

import os
import time
from contextlib import contextmanager
from threading import BoundedSemaphore, Lock
from typing import Any, Iterator

from core.sql_performance import instrumentar_conexao

from psycopg import connect
from psycopg.rows import dict_row

try:
    from psycopg_pool import ConnectionPool
except Exception:  # pragma: no cover - dependência opcional no ambiente local
    ConnectionPool = None

_POOL_LOCK = Lock()
_DB_POOL = None
_DIRECT_FALLBACK_SEMAPHORE = None
_DIRECT_FALLBACK_LIMIT = None
_METRICS_LOCK = Lock()
_METRICS = {
    "pool_aquisicoes": 0,
    "pool_falhas": 0,
    "pool_ativas": 0,
    "pool_ativas_max": 0,
    "pool_conexoes_descartadas": 0,
    "fallback_aquisicoes": 0,
    "fallback_falhas": 0,
    "fallback_ativas": 0,
    "fallback_ativas_max": 0,
    "espera_total_ms": 0.0,
    "espera_max_ms": 0.0,
}


def _registrar_metrica(chave: str, incremento: float = 1.0) -> None:
    with _METRICS_LOCK:
        _METRICS[chave] = _METRICS.get(chave, 0) + incremento


def _registrar_espera(inicio: float) -> None:
    ms = (time.perf_counter() - inicio) * 1000.0
    with _METRICS_LOCK:
        _METRICS["espera_total_ms"] += ms
        _METRICS["espera_max_ms"] = max(_METRICS["espera_max_ms"], ms)


def _alterar_ativas(chave: str, delta: int) -> None:
    """Mantém contadores de conexões em uso sem depender do driver do pool."""
    with _METRICS_LOCK:
        atual = max(0, int(_METRICS.get(chave, 0)) + int(delta))
        _METRICS[chave] = atual
        chave_max = f"{chave}_max"
        _METRICS[chave_max] = max(int(_METRICS.get(chave_max, 0)), atual)


def obter_estatisticas_pool() -> dict[str, Any]:
    """Retorna métricas leves para diagnóstico, sem expor credenciais."""
    with _METRICS_LOCK:
        dados = dict(_METRICS)
    total = dados["pool_aquisicoes"] + dados["fallback_aquisicoes"]
    dados["espera_media_ms"] = round(dados["espera_total_ms"] / total, 2) if total else 0.0
    dados["espera_max_ms"] = round(dados["espera_max_ms"], 2)
    dados["pool_habilitado"] = _pool_habilitado()
    dados["pool_criado"] = _DB_POOL is not None
    try:
        if _DB_POOL is not None and hasattr(_DB_POOL, "get_stats"):
            dados["psycopg_pool"] = dict(_DB_POOL.get_stats())
    except Exception:
        pass
    return dados


def _obter_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL não configurada no ambiente.")
    return url


def _env_int(nome: str, padrao: int, minimo: int | None = None, maximo: int | None = None) -> int:
    try:
        valor = int(os.environ.get(nome, padrao))
    except Exception:
        valor = int(padrao)
    if minimo is not None:
        valor = max(minimo, valor)
    if maximo is not None:
        valor = min(maximo, valor)
    return valor


def _env_float(nome: str, padrao: float, minimo: float | None = None, maximo: float | None = None) -> float:
    try:
        valor = float(os.environ.get(nome, padrao))
    except Exception:
        valor = float(padrao)
    if minimo is not None:
        valor = max(minimo, valor)
    if maximo is not None:
        valor = min(maximo, valor)
    return valor


def _pool_habilitado() -> bool:
    valor_env = os.environ.get("DB_POOL_ENABLED")
    if valor_env is None:
        return True
    return str(valor_env).strip().lower() not in {"0", "false", "no", "off", "nao", "não"}


def _conexao_direta():
    return connect(
        _obter_database_url(),
        row_factory=dict_row,
        sslmode="require",
        connect_timeout=_env_int("DB_CONNECT_TIMEOUT", 8, minimo=3, maximo=30),
        prepare_threshold=None,
    )


def _obter_pool():
    global _DB_POOL
    if ConnectionPool is None or not _pool_habilitado():
        return None
    if _DB_POOL is not None:
        return _DB_POOL
    with _POOL_LOCK:
        if _DB_POOL is not None:
            return _DB_POOL
        min_size = _env_int("DB_POOL_MIN_SIZE", 1, minimo=0, maximo=10)
        max_size = _env_int("DB_POOL_MAX_SIZE", 8, minimo=2, maximo=20)
        if max_size < min_size:
            max_size = min_size or 1
        _DB_POOL = ConnectionPool(
            conninfo=_obter_database_url(),
            kwargs={
                "row_factory": dict_row,
                "sslmode": "require",
                "connect_timeout": _env_int("DB_CONNECT_TIMEOUT", 8, minimo=3, maximo=30),
                "prepare_threshold": None,
            },
            min_size=min_size,
            max_size=max_size,
            timeout=_env_float("DB_POOL_TIMEOUT", 10, minimo=2, maximo=60),
            max_idle=_env_float("DB_POOL_MAX_IDLE", 120, minimo=20, maximo=600),
            max_lifetime=_env_float("DB_POOL_MAX_LIFETIME", 600, minimo=60, maximo=1800),
            reconnect_timeout=_env_float("DB_POOL_RECONNECT_TIMEOUT", 15, minimo=3, maximo=60),
            open=True,
        )
        return _DB_POOL


def _erro_conexao_quebrada(exc: BaseException) -> bool:
    mensagem = repr(exc).lower()
    termos = (
        "ssl syscall error", "ssl error", "eof detected", "bad record mac",
        "consuming input failed", "connection bad", "connection is closed",
        "closed connection", "server closed the connection", "terminating connection",
        "the connection is lost", "pool closed", "network is unreachable",
        "connection timeout expired", "could not translate host name",
    )
    return any(t in mensagem for t in termos)


def _erro_pool_saturado(exc: BaseException) -> bool:
    mensagem = repr(exc).lower()
    return any(t in mensagem for t in (
        "pooltimeout", "couldn't get a connection", "could not get a connection", "pool is full",
    ))


def _conexao_fechada_ou_ruim(conn: Any) -> bool:
    if conn is None:
        return True
    try:
        if bool(getattr(conn, "closed", False)):
            return True
    except Exception:
        return True
    try:
        if bool(getattr(conn, "broken", False)):
            return True
    except Exception:
        pass
    return False


def _validar_conexao_pool(conn: Any) -> bool:
    if _conexao_fechada_ou_ruim(conn):
        return False
    testar = str(os.environ.get("DB_POOL_PING", "1")).strip().lower()
    if testar in {"0", "false", "no", "off", "nao", "não"}:
        return True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception as exc:
        print("AVISO: conexão do pool falhou no ping:", repr(exc), flush=True)
        return False


def fechar_pool(timeout: float = 1) -> None:
    """Fecha e descarta o pool atual; útil após falha SSL ou no encerramento."""
    global _DB_POOL
    with _POOL_LOCK:
        pool = _DB_POOL
        _DB_POOL = None
    try:
        if pool is not None:
            pool.close(timeout=timeout)
    except Exception:
        pass


@contextmanager
def conectar() -> Iterator[Any]:
    """Entrega conexão validada do pool, com fallback direto controlado."""
    global _DIRECT_FALLBACK_SEMAPHORE, _DIRECT_FALLBACK_LIMIT
    inicio_espera = time.perf_counter()
    pool = _obter_pool()
    timeout_pool = _env_float("DB_POOL_TIMEOUT", 10, minimo=1, maximo=30)
    pool_cm = None
    conn_pool = None

    if pool is not None:
        try:
            pool_cm = pool.connection(timeout=timeout_pool)
            conn_pool = pool_cm.__enter__()
            if not _validar_conexao_pool(conn_pool):
                # Uma conexão ociosa pode expirar no Neon sem que o pool inteiro
                # esteja inválido. Descarta somente esta conexão; fechar todo o
                # pool aqui causava uma onda de fallback e deixava o site em fila.
                erro_ping = RuntimeError("Conexão inválida recebida do pool.")
                try:
                    conn_pool.close()
                except Exception:
                    pass
                _registrar_metrica("pool_conexoes_descartadas")
                try:
                    pool_cm.__exit__(RuntimeError, erro_ping, erro_ping.__traceback__)
                except Exception:
                    pass
                pool_cm = None
                conn_pool = None
                raise erro_ping
        except Exception as exc:
            _registrar_metrica("pool_falhas")
            print("AVISO: pool do banco indisponível:", repr(exc), flush=True)
            if _erro_conexao_quebrada(exc) and not _erro_pool_saturado(exc):
                fechar_pool()
            fallback_ligado = str(os.environ.get("DB_DIRECT_FALLBACK_ENABLED", "1")).strip().lower()
            if fallback_ligado in {"0", "false", "no", "off", "nao", "não"}:
                _registrar_espera(inicio_espera)
                raise
        else:
            _registrar_metrica("pool_aquisicoes")
            _registrar_espera(inicio_espera)
            _alterar_ativas("pool_ativas", 1)
            erro_do_bloco = None
            try:
                yield instrumentar_conexao(conn_pool)
            except BaseException as exc:
                erro_do_bloco = exc
                if _erro_conexao_quebrada(exc):
                    try:
                        conn_pool.close()
                    except Exception:
                        pass
                    fechar_pool()
                raise
            finally:
                _alterar_ativas("pool_ativas", -1)
                try:
                    if erro_do_bloco is None:
                        pool_cm.__exit__(None, None, None)
                    else:
                        pool_cm.__exit__(type(erro_do_bloco), erro_do_bloco, erro_do_bloco.__traceback__)
                except Exception as exc:
                    print("AVISO: erro ao devolver conexão ao pool:", repr(exc), flush=True)
                    if _erro_conexao_quebrada(exc):
                        fechar_pool()
            return

    limite_fallback = _env_int("DB_DIRECT_FALLBACK_MAX", 1, minimo=0, maximo=6)
    if limite_fallback <= 0:
        _registrar_espera(inicio_espera)
        raise RuntimeError("Pool do banco indisponível e fallback direto desativado.")
    if _DIRECT_FALLBACK_SEMAPHORE is None or _DIRECT_FALLBACK_LIMIT != limite_fallback:
        with _POOL_LOCK:
            if _DIRECT_FALLBACK_SEMAPHORE is None or _DIRECT_FALLBACK_LIMIT != limite_fallback:
                # Permite que alterações de configuração sejam aplicadas em
                # novo processo/testes sem conservar um limite antigo.
                _DIRECT_FALLBACK_SEMAPHORE = BoundedSemaphore(limite_fallback)
                _DIRECT_FALLBACK_LIMIT = limite_fallback
    adquiriu = _DIRECT_FALLBACK_SEMAPHORE.acquire(
        timeout=_env_float("DB_DIRECT_FALLBACK_TIMEOUT", 3, minimo=0.2, maximo=10)
    )
    if not adquiriu:
        _registrar_metrica("fallback_falhas")
        _registrar_espera(inicio_espera)
        raise RuntimeError("Banco ocupado: pool indisponível e limite de conexões diretas atingido.")

    conn = None
    try:
        print("AVISO: usando conexão direta controlada fora do pool", flush=True)
        conn = _conexao_direta()
        _registrar_metrica("fallback_aquisicoes")
        _registrar_espera(inicio_espera)
        _alterar_ativas("fallback_ativas", 1)
        yield instrumentar_conexao(conn)
    except BaseException as exc:
        if _erro_conexao_quebrada(exc):
            fechar_pool()
        raise
    finally:
        if conn is not None:
            _alterar_ativas("fallback_ativas", -1)
        try:
            if conn is not None:
                conn.close()
        finally:
            try:
                _DIRECT_FALLBACK_SEMAPHORE.release()
            except Exception:
                pass
