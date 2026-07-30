#!/usr/bin/env python3
"""Gera o relatório final de homologação do VolleyTablePro."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.release_readiness import build_release_readiness_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auditoria final de prontidão do VolleyTablePro")
    parser.add_argument("--base-url", default=os.getenv("VTP_RELEASE_BASE_URL", ""), help="URL do serviço de homologação")
    parser.add_argument("--load-report", default=os.getenv("VTP_RELEASE_LOAD_REPORT", ""), help="JSON gerado pelo ensaio de carga")
    parser.add_argument("--admin-cookie", default=os.getenv("VTP_RELEASE_ADMIN_COOKIE", ""), help="Cookie opcional do Super ADM; nunca é gravado no relatório")
    parser.add_argument("--output-dir", default="release_reports", help="Diretório dos relatórios")
    parser.add_argument("--strict-warnings", action="store_true", help="Retorna falha também quando houver avisos")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_release_readiness_report(
        base_url=args.base_url,
        load_report_path=args.load_report or None,
        admin_cookie=args.admin_cookie,
    )
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "prontidao_producao.json"
    md_path = output / "prontidao_producao.md"
    json_path.write_text(json.dumps(report.as_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(report.to_markdown(), encoding="utf-8")
    print(report.to_markdown())
    print(f"JSON: {json_path.resolve()}")
    print(f"Markdown: {md_path.resolve()}")
    if not report.approved:
        return 1
    if args.strict_warnings and report.warnings:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
