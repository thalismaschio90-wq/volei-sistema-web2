"""Compara dois arquivos JSON exportados por /admin/performance/exportar.json."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from core.performance_compare import comparar_snapshots, exportar_markdown_comparacao


def main() -> int:
    parser = argparse.ArgumentParser(description="Compara métricas antes/depois do VolleyTablePro")
    parser.add_argument("antes", type=Path)
    parser.add_argument("depois", type=Path)
    parser.add_argument("--saida", type=Path, default=Path("performance_comparacao.md"))
    args = parser.parse_args()

    antes = json.loads(args.antes.read_text(encoding="utf-8"))
    depois = json.loads(args.depois.read_text(encoding="utf-8"))
    resultado = comparar_snapshots(antes, depois)
    args.saida.write_text(exportar_markdown_comparacao(resultado), encoding="utf-8")
    print(f"Relatório criado em: {args.saida}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
