from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


def _markdown_table(summary: dict[str, Any]) -> str:
    lines = [
        "| Métrica | Total | Falhas | Média | P95 | P99 | Máximo |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for name, item in summary.items():
        lines.append(
            f"| `{name}` | {item['count']} | {item['failed']} | "
            f"{item['avg_ms']:.3f} ms | {item['p95_ms']:.3f} ms | "
            f"{item['p99_ms']:.3f} ms | {item['max_ms']:.3f} ms |"
        )
    return "\n".join(lines)


def write_report(report_dir: str, payload: dict[str, Any]) -> tuple[Path, Path]:
    target = Path(report_dir)
    target.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = target / f"vtp_load_{stamp}.json"
    md_path = target / f"vtp_load_{stamp}.md"

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    config = payload.get("config", {})
    conclusions = payload.get("conclusions", {})
    md = [
        "# Relatório de carga — VolleyTablePro",
        "",
        f"- Gerado em: `{payload.get('generated_at', '')}`",
        f"- URL: `{config.get('base_url', '')}`",
        f"- Partida de homologação: `{config.get('partida_id', '')}`",
        f"- Visualizadores simulados: `{config.get('viewers', 0)}`",
        f"- Duração: `{config.get('duration_seconds', 0)} s`",
        f"- Escritas habilitadas: `{config.get('allow_writes', False)}`",
        f"- Sockets públicos simulados: `{config.get('socket_viewers', 0)}`",
        f"- Código público: `{config.get('public_code', '') or 'não informado'}`",
        "",
        "## Pré-validação",
        "",
        "```json",
        json.dumps(payload.get("preflight", {}), ensure_ascii=False, indent=2),
        "```",
        "",
        "## Latências",
        "",
        _markdown_table(payload.get("metrics", {})),
        "",
        "## Sincronização Socket.IO",
        "",
        "```json",
        json.dumps(payload.get("socket_observations", []), ensure_ascii=False, indent=2),
        "```",
        "",

        "## Infraestrutura e métricas internas",
        "",
        "```json",
        json.dumps(payload.get("admin_metrics", {}), ensure_ascii=False, indent=2),
        "```",
        "",
        "## Resultado",
        "",
        f"- Aprovado: **{'SIM' if conclusions.get('approved') else 'NÃO'}**",
    ]
    for reason in conclusions.get("reasons", []):
        md.append(f"- {reason}")
    md_path.write_text("\n".join(md) + "\n", encoding="utf-8")
    return json_path, md_path
