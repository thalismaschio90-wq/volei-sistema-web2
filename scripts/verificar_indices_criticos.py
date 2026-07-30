"""Confirma se os índices críticos aparecem nos planos do PostgreSQL.

Por padrão usa EXPLAIN sem executar novamente as consultas. O modo --analyze
é permitido somente com SQL_INDEX_VERIFY_ANALYZE_ALLOWED=1 e deve ser usado
apenas em homologação.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.index_verification import construir_verificacoes, executar_explain  # noqa: E402


def _verdadeiro(nome: str) -> bool:
    return os.getenv(nome, "0").strip().lower() in {"1", "true", "yes", "on"}


def _markdown(payload: dict) -> str:
    linhas = [
        "# Verificação dos índices críticos",
        "",
        f"Gerado em: {payload['gerado_em']}",
        f"Modo ANALYZE: {'sim' if payload['analyze'] else 'não'}",
        "",
        "| Verificação | Índice esperado | Usado | Custo | Linhas | Tempo |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for item in payload["resultados"]:
        linhas.append(
            f"| {item['nome']} | `{item['indice_esperado']}` | "
            f"{'sim' if item['indice_usado'] else 'não'} | "
            f"{item['custo_total'] if item['custo_total'] is not None else '-'} | "
            f"{item['linhas_estimadas'] if item['linhas_estimadas'] is not None else '-'} | "
            f"{item['tempo_execucao_ms'] if item['tempo_execucao_ms'] is not None else '-'} |"
        )
    for item in payload["resultados"]:
        linhas.extend([
            "",
            f"## {item['nome']}",
            "",
            item["descricao"],
            "",
            f"- Índices encontrados: {', '.join(item['indices_encontrados']) or 'nenhum'}",
            f"- Nós do plano: {', '.join(item['tipos_nos']) or 'nenhum'}",
            f"- Seq scans: {', '.join(item['seq_scans']) or 'nenhum'}",
        ])
        linhas.extend(f"- {obs}" for obs in item["observacoes"])
    linhas.extend([
        "",
        "## Interpretação",
        "",
        "Um índice não aparecer não significa automaticamente erro. Em tabelas pequenas, "
        "o PostgreSQL pode escolher Seq Scan por ser mais barato. Compare também custo, "
        "linhas e tempo com o laboratório SQL da Sprint 73.",
    ])
    return "\n".join(linhas) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analyze", action="store_true")
    parser.add_argument("--saida", default="sql_lab_reports/verificacao_indices")
    args = parser.parse_args()

    competicao = os.getenv("VTP_LAB_COMPETICAO", "").strip()
    equipe = os.getenv("VTP_LAB_EQUIPE", "").strip()
    partida_raw = os.getenv("VTP_LAB_PARTIDA_ID", "").strip()
    if not competicao or not equipe or not partida_raw.isdigit():
        print("ERRO: defina VTP_LAB_COMPETICAO, VTP_LAB_EQUIPE e VTP_LAB_PARTIDA_ID.", file=sys.stderr)
        return 2
    if args.analyze and not _verdadeiro("SQL_INDEX_VERIFY_ANALYZE_ALLOWED"):
        print("ERRO: --analyze exige SQL_INDEX_VERIFY_ANALYZE_ALLOWED=1.", file=sys.stderr)
        return 3

    from repositories.conexao import conectar

    verificacoes = construir_verificacoes(
        competicao=competicao,
        equipe=equipe,
        partida_id=int(partida_raw),
    )
    resultados = []
    conn = conectar()
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '3000ms'")
            for verificacao in verificacoes:
                print(f"Verificando {verificacao.nome}...")
                resultados.append(executar_explain(cur, verificacao, analyze=args.analyze).to_dict())
        conn.rollback()
    finally:
        conn.close()

    payload = {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "analyze": bool(args.analyze),
        "resumo": {
            "total": len(resultados),
            "indices_usados": sum(1 for item in resultados if item["indice_usado"]),
        },
        "resultados": resultados,
    }
    base = ROOT / args.saida
    base.parent.mkdir(parents=True, exist_ok=True)
    base.with_suffix(".json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    base.with_suffix(".md").write_text(_markdown(payload), encoding="utf-8")
    print(f"Relatórios: {base.with_suffix('.json')} e {base.with_suffix('.md')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
