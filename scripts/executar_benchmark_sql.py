"""Executa um cenário JSON de benchmark em ambiente de homologação."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.sql_benchmark import executar_cenario, salvar_resultados


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark controlado do VolleyTablePro")
    parser.add_argument("cenario", help="Arquivo JSON com os benchmarks")
    parser.add_argument("--saida-json", default="benchmark_reports/benchmark.json")
    parser.add_argument("--saida-md", default="benchmark_reports/benchmark.md")
    args = parser.parse_args()

    cenario = json.loads(Path(args.cenario).read_text(encoding="utf-8"))
    resultados = executar_cenario(cenario)
    Path(args.saida_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.saida_md).parent.mkdir(parents=True, exist_ok=True)
    salvar_resultados(resultados, args.saida_json, args.saida_md, titulo=str(cenario.get("titulo") or "Benchmark PostgreSQL"))

    erros = [r for r in resultados if r.erro]
    print(f"Benchmarks concluídos: {len(resultados)}; erros: {len(erros)}")
    print(f"JSON: {args.saida_json}")
    print(f"Markdown: {args.saida_md}")
    return 1 if erros else 0


if __name__ == "__main__":
    raise SystemExit(main())
