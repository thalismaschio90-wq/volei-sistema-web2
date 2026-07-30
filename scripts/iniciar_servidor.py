#!/usr/bin/env python3
"""Executa migrações uma vez e substitui o processo pelo Gunicorn."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "sim"}


def main() -> int:
    if _env_bool("DB_MIGRATIONS_ON_START", True):
        command = [sys.executable, str(ROOT / "scripts" / "executar_migracoes.py")]
        completed = subprocess.run(command, cwd=ROOT, check=False)
        if completed.returncode != 0:
            print("Migrações falharam; o Gunicorn não será iniciado.", file=sys.stderr)
            return completed.returncode

    os.chdir(ROOT)
    os.execvp("gunicorn", ["gunicorn", "app:app", "-c", "gunicorn.conf.py"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
