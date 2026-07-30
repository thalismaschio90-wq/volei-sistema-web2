from __future__ import annotations

import argparse
import json
from pathlib import Path

from core.sql_lab import executar_laboratorio, salvar_laboratorio


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa laboratório SQL baseline/candidato em homologação.")
    parser.add_argument("config", help="Arquivo JSON do laboratório.")
    parser.add_argument("--saida", default="sql_lab_reports/laboratorio", help="Prefixo dos arquivos de saída.")
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    resultado = executar_laboratorio(config)
    prefixo = Path(args.saida)
    prefixo.parent.mkdir(parents=True, exist_ok=True)
    salvar_laboratorio(resultado, prefixo.with_suffix(".json"), prefixo.with_suffix(".md"))
    print(prefixo.with_suffix(".md"))
    return 0 if resultado.get("aprovado") else 2


if __name__ == "__main__":
    raise SystemExit(main())
