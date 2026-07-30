import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from core.sql_benchmark import (
    BenchmarkError,
    benchmark_callable,
    benchmark_sql,
    executar_cenario,
    percentil,
    relatorio_markdown,
)


def test_percentil_amostra_pequena():
    assert percentil([], 95) == 0.0
    assert percentil([10], 95) == 10.0
    assert percentil([1, 2, 3, 4], 50) == 2.5


def test_benchmark_callable_produz_metricas():
    resultado = benchmark_callable("soma", lambda a, b: a + b, args=[1, 2], iteracoes=5, aquecimentos=1)
    assert resultado.erro == ""
    assert resultado.iteracoes == 5
    assert len(resultado.amostras_ms) == 5
    assert resultado.maximo_ms >= resultado.minimo_ms


def test_sql_benchmark_bloqueado_por_padrao(monkeypatch):
    monkeypatch.delenv("SQL_BENCHMARK_ALLOW_DATABASE", raising=False)
    with pytest.raises(BenchmarkError):
        benchmark_sql("teste", "SELECT 1")


def test_sql_benchmark_rejeita_escrita(monkeypatch):
    monkeypatch.setenv("SQL_BENCHMARK_ALLOW_DATABASE", "1")
    with pytest.raises(BenchmarkError):
        benchmark_sql("teste", "DELETE FROM partidas")


def test_cenario_registra_erro_sem_interromper():
    resultados = executar_cenario({"benchmarks": [{"nome": "invalido", "tipo": "callable", "callable": "modulo.inexistente:funcao"}]})
    assert len(resultados) == 1
    assert resultados[0].erro


def test_markdown_nao_expoe_argumentos():
    resultado = benchmark_callable("seguro", lambda segredo: 1, args=["cpf-123"], iteracoes=1, aquecimentos=0)
    texto = relatorio_markdown([resultado])
    assert "cpf-123" not in texto
    assert "seguro" in texto
