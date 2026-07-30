"""Benchmark controlado de funções e consultas de leitura.

Este módulo foi pensado para homologação. Ele não executa escrita SQL, não
armazena parâmetros no relatório e não deve ser habilitado automaticamente em
produção.
"""
from __future__ import annotations

import importlib
import json
import math
import os
import statistics
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from repositories.conexao import conectar, obter_estatisticas_pool


class BenchmarkError(RuntimeError):
    """Erro de configuração ou execução do benchmark."""


@dataclass
class AmostraBenchmark:
    duracao_ms: float
    linhas: int | None = None


@dataclass
class ResultadoBenchmark:
    nome: str
    tipo: str
    iteracoes: int
    aquecimentos: int
    amostras_ms: list[float] = field(default_factory=list)
    linhas_ultima_execucao: int | None = None
    media_ms: float = 0.0
    mediana_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    minimo_ms: float = 0.0
    maximo_ms: float = 0.0
    desvio_padrao_ms: float = 0.0
    pool_antes: dict[str, Any] = field(default_factory=dict)
    pool_depois: dict[str, Any] = field(default_factory=dict)
    erro: str = ""

    def como_dict(self) -> dict[str, Any]:
        return asdict(self)


def _env_bool(nome: str, padrao: bool = False) -> bool:
    valor = os.environ.get(nome)
    if valor is None:
        return padrao
    return str(valor).strip().lower() in {"1", "true", "yes", "on", "sim"}


def percentil(valores: Sequence[float], percentual: float) -> float:
    """Percentil linear inclusivo, estável para amostras pequenas."""
    if not valores:
        return 0.0
    ordenados = sorted(float(v) for v in valores)
    if len(ordenados) == 1:
        return ordenados[0]
    p = min(100.0, max(0.0, float(percentual))) / 100.0
    posicao = (len(ordenados) - 1) * p
    inferior = math.floor(posicao)
    superior = math.ceil(posicao)
    if inferior == superior:
        return ordenados[inferior]
    peso = posicao - inferior
    return ordenados[inferior] + (ordenados[superior] - ordenados[inferior]) * peso


def _resumir(nome: str, tipo: str, amostras: Sequence[AmostraBenchmark], *, iteracoes: int, aquecimentos: int, pool_antes: Mapping[str, Any] | None = None, pool_depois: Mapping[str, Any] | None = None) -> ResultadoBenchmark:
    tempos = [round(float(a.duracao_ms), 3) for a in amostras]
    linhas = amostras[-1].linhas if amostras else None
    return ResultadoBenchmark(
        nome=nome,
        tipo=tipo,
        iteracoes=iteracoes,
        aquecimentos=aquecimentos,
        amostras_ms=tempos,
        linhas_ultima_execucao=linhas,
        media_ms=round(statistics.fmean(tempos), 3) if tempos else 0.0,
        mediana_ms=round(statistics.median(tempos), 3) if tempos else 0.0,
        p95_ms=round(percentil(tempos, 95), 3),
        p99_ms=round(percentil(tempos, 99), 3),
        minimo_ms=round(min(tempos), 3) if tempos else 0.0,
        maximo_ms=round(max(tempos), 3) if tempos else 0.0,
        desvio_padrao_ms=round(statistics.pstdev(tempos), 3) if len(tempos) > 1 else 0.0,
        pool_antes=dict(pool_antes or {}),
        pool_depois=dict(pool_depois or {}),
    )


def importar_callable(caminho: str) -> Callable[..., Any]:
    """Importa ``pacote.modulo:funcao`` sem executar código arbitrário textual."""
    modulo_nome, separador, atributo = str(caminho or "").partition(":")
    if not separador or not modulo_nome or not atributo:
        raise BenchmarkError("Callable deve usar o formato pacote.modulo:funcao.")
    modulo = importlib.import_module(modulo_nome)
    alvo: Any = modulo
    for parte in atributo.split("."):
        alvo = getattr(alvo, parte)
    if not callable(alvo):
        raise BenchmarkError(f"O alvo {caminho!r} não é chamável.")
    return alvo


def benchmark_callable(nome: str, funcao: Callable[..., Any], *, args: Sequence[Any] | None = None, kwargs: Mapping[str, Any] | None = None, iteracoes: int = 10, aquecimentos: int = 2) -> ResultadoBenchmark:
    iteracoes = max(1, min(int(iteracoes), 1000))
    aquecimentos = max(0, min(int(aquecimentos), 100))
    args = tuple(args or ())
    kwargs = dict(kwargs or {})

    for _ in range(aquecimentos):
        funcao(*args, **kwargs)

    pool_antes = obter_estatisticas_pool()
    amostras: list[AmostraBenchmark] = []
    for _ in range(iteracoes):
        inicio = time.perf_counter()
        retorno = funcao(*args, **kwargs)
        duracao = (time.perf_counter() - inicio) * 1000.0
        linhas = len(retorno) if isinstance(retorno, (list, tuple, dict, set)) else None
        amostras.append(AmostraBenchmark(duracao_ms=duracao, linhas=linhas))
    pool_depois = obter_estatisticas_pool()
    return _resumir(nome, "callable", amostras, iteracoes=iteracoes, aquecimentos=aquecimentos, pool_antes=pool_antes, pool_depois=pool_depois)


def _validar_sql_leitura(sql: str) -> str:
    limpo = str(sql or "").strip()
    primeira = limpo.split(None, 1)[0].upper() if limpo else ""
    if primeira not in {"SELECT", "WITH", "EXPLAIN"}:
        raise BenchmarkError("O benchmark SQL aceita somente SELECT, WITH ou EXPLAIN.")
    proibidos = (" INSERT ", " UPDATE ", " DELETE ", " ALTER ", " DROP ", " CREATE ", " TRUNCATE ", " GRANT ", " REVOKE ")
    normalizado = " " + " ".join(limpo.upper().split()) + " "
    if any(token in normalizado for token in proibidos):
        raise BenchmarkError("A consulta contém uma operação de escrita ou DDL.")
    return limpo


def benchmark_sql(nome: str, sql: str, *, params: Sequence[Any] | Mapping[str, Any] | None = None, iteracoes: int = 5, aquecimentos: int = 1, statement_timeout_ms: int = 5000, fetch_limit: int = 10000) -> ResultadoBenchmark:
    """Executa consulta de leitura em homologação e sempre encerra com rollback."""
    if not _env_bool("SQL_BENCHMARK_ALLOW_DATABASE", False):
        raise BenchmarkError("Defina SQL_BENCHMARK_ALLOW_DATABASE=1 somente em homologação.")
    sql = _validar_sql_leitura(sql)
    iteracoes = max(1, min(int(iteracoes), 100))
    aquecimentos = max(0, min(int(aquecimentos), 20))
    timeout = max(100, min(int(statement_timeout_ms), 60000))
    fetch_limit = max(1, min(int(fetch_limit), 100000))

    def executar_uma() -> AmostraBenchmark:
        with conectar() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = %s", (timeout,))
                    inicio = time.perf_counter()
                    cur.execute(sql, params)
                    linhas = cur.fetchmany(fetch_limit)
                    duracao = (time.perf_counter() - inicio) * 1000.0
                conn.rollback()
                return AmostraBenchmark(duracao_ms=duracao, linhas=len(linhas))
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise

    for _ in range(aquecimentos):
        executar_uma()
    pool_antes = obter_estatisticas_pool()
    amostras = [executar_uma() for _ in range(iteracoes)]
    pool_depois = obter_estatisticas_pool()
    return _resumir(nome, "sql", amostras, iteracoes=iteracoes, aquecimentos=aquecimentos, pool_antes=pool_antes, pool_depois=pool_depois)


def executar_cenario(cenario: Mapping[str, Any]) -> list[ResultadoBenchmark]:
    resultados: list[ResultadoBenchmark] = []
    for indice, item in enumerate(cenario.get("benchmarks") or [], start=1):
        nome = str(item.get("nome") or f"benchmark_{indice}")
        tipo = str(item.get("tipo") or "callable").lower()
        try:
            if tipo == "callable":
                funcao = importar_callable(str(item.get("callable") or ""))
                resultado = benchmark_callable(
                    nome,
                    funcao,
                    args=item.get("args") or [],
                    kwargs=item.get("kwargs") or {},
                    iteracoes=int(item.get("iteracoes", 10)),
                    aquecimentos=int(item.get("aquecimentos", 2)),
                )
            elif tipo == "sql":
                sql = item.get("sql")
                if not sql and item.get("sql_arquivo"):
                    sql = Path(str(item["sql_arquivo"])).read_text(encoding="utf-8")
                resultado = benchmark_sql(
                    nome,
                    str(sql or ""),
                    params=item.get("params"),
                    iteracoes=int(item.get("iteracoes", 5)),
                    aquecimentos=int(item.get("aquecimentos", 1)),
                    statement_timeout_ms=int(item.get("statement_timeout_ms", 5000)),
                    fetch_limit=int(item.get("fetch_limit", 10000)),
                )
            else:
                raise BenchmarkError(f"Tipo de benchmark desconhecido: {tipo}")
        except Exception as exc:
            resultado = ResultadoBenchmark(nome=nome, tipo=tipo, iteracoes=0, aquecimentos=0, erro=f"{type(exc).__name__}: {exc}")
        resultados.append(resultado)
    return resultados


def relatorio_markdown(resultados: Iterable[ResultadoBenchmark], titulo: str = "Benchmark PostgreSQL") -> str:
    itens = list(resultados)
    linhas = [f"# {titulo}", "", "| Benchmark | Tipo | Média | P95 | P99 | Máximo | Iterações | Linhas | Status |", "|---|---:|---:|---:|---:|---:|---:|---:|---|"]
    for r in itens:
        status = f"ERRO: {r.erro}" if r.erro else "OK"
        linhas.append(f"| {r.nome} | {r.tipo} | {r.media_ms:.3f} ms | {r.p95_ms:.3f} ms | {r.p99_ms:.3f} ms | {r.maximo_ms:.3f} ms | {r.iteracoes} | {r.linhas_ultima_execucao if r.linhas_ultima_execucao is not None else '-'} | {status} |")
    linhas.extend(["", "## Observações", "", "- Execute apenas em homologação com dados representativos.", "- Parâmetros SQL não são incluídos no relatório.", "- Compare o mesmo cenário antes e depois de cada índice ou reescrita.", "- Use `EXPLAIN (ANALYZE, BUFFERS)` manualmente apenas nas consultas prioritárias."])
    return "\n".join(linhas) + "\n"


def salvar_resultados(resultados: Sequence[ResultadoBenchmark], destino_json: str | Path, destino_md: str | Path | None = None, *, titulo: str = "Benchmark PostgreSQL") -> None:
    Path(destino_json).write_text(json.dumps({"titulo": titulo, "resultados": [r.como_dict() for r in resultados]}, ensure_ascii=False, indent=2), encoding="utf-8")
    if destino_md:
        Path(destino_md).write_text(relatorio_markdown(resultados, titulo=titulo), encoding="utf-8")
