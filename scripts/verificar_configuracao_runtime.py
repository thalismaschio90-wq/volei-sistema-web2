#!/usr/bin/env python3
"""Valida variáveis do Render antes do deploy ou da troca de workers."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.runtime_config import load_runtime_config


def main() -> int:
    config = load_runtime_config()
    print(json.dumps(config.public_dict(), ensure_ascii=False, indent=2))
    return 0 if not config.errors() else 1


if __name__ == "__main__":
    raise SystemExit(main())
