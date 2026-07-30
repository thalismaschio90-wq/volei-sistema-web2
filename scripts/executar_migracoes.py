#!/usr/bin/env python3
"""Executa as migrações versionadas antes de iniciar a aplicação."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.schema_migrations import executar_migracoes


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa migrações do VolleyTablePro.")
    parser.add_argument("--dry-run", action="store_true", help="Lista as etapas sem acessar ou alterar o banco.")
    parser.add_argument("--force", action="store_true", help="Executa novamente etapas já registradas.")
    parser.add_argument("--json", action="store_true", help="Imprime o resultado em JSON.")
    args = parser.parse_args()

    try:
        resultado = executar_migracoes(dry_run=args.dry_run, force=args.force)
    except Exception as exc:
        print(f"ERRO ao executar migrações: {exc!r}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(resultado, ensure_ascii=False, indent=2))
    else:
        for item in resultado:
            print(f"[{item['status']}] {item['version']} - {item['description']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
