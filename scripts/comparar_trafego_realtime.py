from __future__ import annotations

import argparse
import json
from pathlib import Path

from core.realtime_compare import comparar_snapshots_realtime, gerar_markdown_realtime


def main() -> int:
    parser = argparse.ArgumentParser(description="Compara dois JSONs exportados de /admin/realtime-delta-status")
    parser.add_argument("antes")
    parser.add_argument("depois")
    parser.add_argument("--saida", default="comparacao_trafego_realtime.md")
    args = parser.parse_args()

    antes = json.loads(Path(args.antes).read_text(encoding="utf-8"))
    depois = json.loads(Path(args.depois).read_text(encoding="utf-8"))
    comparacao = comparar_snapshots_realtime(antes, depois)
    Path(args.saida).write_text(gerar_markdown_realtime(comparacao), encoding="utf-8")
    print(f"Relatório salvo em {args.saida}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
