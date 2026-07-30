"""Comparação segura entre dois snapshots de performance.

Usado para medir antes/depois sem armazenar SQL bruto ou parâmetros.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _numero(valor: Any) -> float:
    try:
        return float(valor or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _variacao_percentual(antes: float, depois: float) -> float | None:
    if antes <= 0:
        return None
    return round(((depois - antes) / antes) * 100.0, 2)


def _indice_por(chave: str, itens: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    resultado: dict[str, dict[str, Any]] = {}
    for item in itens:
        valor = str(item.get(chave) or "").strip()
        if valor:
            resultado[valor] = item
    return resultado


def comparar_snapshots(antes: dict[str, Any], depois: dict[str, Any]) -> dict[str, Any]:
    """Compara snapshots exportados pelo painel de performance."""
    rotas_antes = _indice_por("endpoint", list(antes.get("rotas") or antes.get("rotas_prioritarias") or []))
    rotas_depois = _indice_por("endpoint", list(depois.get("rotas") or depois.get("rotas_prioritarias") or []))

    comparacao_rotas: list[dict[str, Any]] = []
    for endpoint in sorted(set(rotas_antes) | set(rotas_depois)):
        a = rotas_antes.get(endpoint, {})
        d = rotas_depois.get(endpoint, {})
        p95_a = _numero(a.get("duracao_p95_ms"))
        p95_d = _numero(d.get("duracao_p95_ms"))
        sql_a = _numero(a.get("sql_media_ms"))
        sql_d = _numero(d.get("sql_media_ms"))
        comparacao_rotas.append({
            "endpoint": endpoint,
            "metodo": d.get("metodo") or a.get("metodo") or "",
            "p95_antes_ms": p95_a,
            "p95_depois_ms": p95_d,
            "p95_variacao_pct": _variacao_percentual(p95_a, p95_d),
            "sql_antes_ms": sql_a,
            "sql_depois_ms": sql_d,
            "sql_variacao_pct": _variacao_percentual(sql_a, sql_d),
            "consultas_antes": _numero(a.get("sql_media_consultas")),
            "consultas_depois": _numero(d.get("sql_media_consultas")),
            "amostras_antes": int(_numero(a.get("quantidade"))),
            "amostras_depois": int(_numero(d.get("quantidade"))),
        })

    consultas_antes = _indice_por(
        "fingerprint", list(antes.get("consultas_lentas") or antes.get("consultas_prioritarias") or [])
    )
    consultas_depois = _indice_por(
        "fingerprint", list(depois.get("consultas_lentas") or depois.get("consultas_prioritarias") or [])
    )
    comparacao_consultas: list[dict[str, Any]] = []
    for fingerprint in sorted(set(consultas_antes) | set(consultas_depois)):
        a = consultas_antes.get(fingerprint, {})
        d = consultas_depois.get(fingerprint, {})
        media_a = _numero(a.get("duracao_media_ms"))
        media_d = _numero(d.get("duracao_media_ms"))
        comparacao_consultas.append({
            "fingerprint": fingerprint,
            "operacao": d.get("operacao") or a.get("operacao") or "SQL",
            "media_antes_ms": media_a,
            "media_depois_ms": media_d,
            "variacao_pct": _variacao_percentual(media_a, media_d),
            "max_antes_ms": _numero(a.get("duracao_max_ms")),
            "max_depois_ms": _numero(d.get("duracao_max_ms")),
            "ocorrencias_antes": int(_numero(a.get("quantidade"))),
            "ocorrencias_depois": int(_numero(d.get("quantidade"))),
            "origens": d.get("origens") or a.get("origens") or [],
        })

    comparacao_rotas.sort(
        key=lambda x: (
            x["p95_variacao_pct"] is None,
            -(x["p95_variacao_pct"] or 0),
            -x["p95_depois_ms"],
        )
    )
    comparacao_consultas.sort(
        key=lambda x: (
            x["variacao_pct"] is None,
            -(x["variacao_pct"] or 0),
            -x["media_depois_ms"],
        )
    )

    melhoras = [x for x in comparacao_rotas if x["p95_variacao_pct"] is not None and x["p95_variacao_pct"] < 0]
    regressoes = [x for x in comparacao_rotas if x["p95_variacao_pct"] is not None and x["p95_variacao_pct"] > 10]
    return {
        "ok": True,
        "resumo": {
            "rotas_comparadas": len(comparacao_rotas),
            "consultas_comparadas": len(comparacao_consultas),
            "rotas_melhoraram": len(melhoras),
            "rotas_regrediram_mais_10pct": len(regressoes),
        },
        "rotas": comparacao_rotas,
        "consultas": comparacao_consultas,
    }


def exportar_markdown_comparacao(resultado: dict[str, Any]) -> str:
    resumo = resultado.get("resumo") or {}
    linhas = [
        "# Comparação de performance — VolleyTablePro",
        "",
        f"- Rotas comparadas: **{resumo.get('rotas_comparadas', 0)}**",
        f"- Consultas comparadas: **{resumo.get('consultas_comparadas', 0)}**",
        f"- Rotas que melhoraram: **{resumo.get('rotas_melhoraram', 0)}**",
        f"- Regressões acima de 10%: **{resumo.get('rotas_regrediram_mais_10pct', 0)}**",
        "",
        "## Rotas",
        "",
        "| Endpoint | P95 antes | P95 depois | Variação | SQL antes | SQL depois |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for item in resultado.get("rotas") or []:
        variacao = item.get("p95_variacao_pct")
        texto_var = "n/d" if variacao is None else f"{variacao:+.1f}%"
        linhas.append(
            f"| `{item.get('endpoint','')}` | {item.get('p95_antes_ms',0):.1f} ms | "
            f"{item.get('p95_depois_ms',0):.1f} ms | {texto_var} | "
            f"{item.get('sql_antes_ms',0):.1f} ms | {item.get('sql_depois_ms',0):.1f} ms |"
        )
    linhas.extend([
        "",
        "## Consultas",
        "",
        "| Fingerprint | Média antes | Média depois | Variação | Ocorrências antes/depois |",
        "|---|---:|---:|---:|---:|",
    ])
    for item in resultado.get("consultas") or []:
        variacao = item.get("variacao_pct")
        texto_var = "n/d" if variacao is None else f"{variacao:+.1f}%"
        linhas.append(
            f"| `{item.get('fingerprint','')}` | {item.get('media_antes_ms',0):.1f} ms | "
            f"{item.get('media_depois_ms',0):.1f} ms | {texto_var} | "
            f"{item.get('ocorrencias_antes',0)}/{item.get('ocorrencias_depois',0)} |"
        )
    linhas.append("")
    return "\n".join(linhas)
