#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.load.config import LoadTestConfig
from tests.load.scenario import ChampionshipLoadScenario


def main() -> int:
    config = LoadTestConfig.from_env()
    print("VolleyTablePro — laboratório de carga")
    print(f"Destino: {config.base_url}")
    print(f"Partida: {config.partida_id} | Visualizadores HTTP: {config.viewers} | Sockets públicos: {config.socket_viewers}")
    print(f"Escritas habilitadas: {'SIM' if config.allow_writes else 'NÃO'}")
    print(f"Métricas administrativas: {'SIM' if config.collect_admin_metrics else 'NÃO'}")
    if not config.allow_writes:
        print("Modo seguro: somente leitura e conexões em tempo real.")

    try:
        json_path, md_path = ChampionshipLoadScenario(config).run()
    except Exception as exc:
        print(f"FALHA: {exc}", file=sys.stderr)
        return 1

    print(f"Relatório JSON: {json_path}")
    print(f"Relatório Markdown: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
