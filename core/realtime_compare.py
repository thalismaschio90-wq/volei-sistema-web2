"""Comparação segura de dois snapshots do painel de tempo real."""
from __future__ import annotations

from typing import Any


def _numero(valor: Any) -> float:
    try:
        return float(valor or 0)
    except (TypeError, ValueError):
        return 0.0


def _variacao_percentual(antes: float, depois: float) -> float | None:
    if antes == 0:
        return None
    return round(100.0 * (depois - antes) / antes, 2)


def comparar_snapshots_realtime(antes: dict[str, Any], depois: dict[str, Any]) -> dict[str, Any]:
    a = dict((antes or {}).get("despacho") or {})
    d = dict((depois or {}).get("despacho") or {})
    campos = [
        "bytes_recebidos_estimados",
        "bytes_emitidos_estimados",
        "bytes_economizados_despacho",
        "duplicados_descartados",
        "agrupados",
        "emitidos_critica",
        "emitidos_normal",
        "emitidos_baixa",
    ]
    metricas: dict[str, Any] = {}
    for campo in campos:
        av = _numero(a.get(campo))
        dv = _numero(d.get(campo))
        metricas[campo] = {
            "antes": av,
            "depois": dv,
            "diferenca": round(dv - av, 3),
            "variacao_percentual": _variacao_percentual(av, dv),
        }

    def eventos(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
        itens = ((snapshot or {}).get("despacho") or {}).get("eventos_por_trafego") or []
        return {str(i.get("evento")): dict(i) for i in itens if isinstance(i, dict) and i.get("evento")}

    ea, ed = eventos(antes), eventos(depois)
    comparacao_eventos = []
    for nome in sorted(set(ea) | set(ed)):
        bytes_a = _numero(ea.get(nome, {}).get("bytes"))
        bytes_d = _numero(ed.get(nome, {}).get("bytes"))
        comparacao_eventos.append({
            "evento": nome,
            "bytes_antes": bytes_a,
            "bytes_depois": bytes_d,
            "diferenca": round(bytes_d - bytes_a, 3),
            "variacao_percentual": _variacao_percentual(bytes_a, bytes_d),
        })
    comparacao_eventos.sort(key=lambda x: abs(float(x["diferenca"])), reverse=True)

    return {
        "metricas": metricas,
        "eventos": comparacao_eventos,
        "observacao": "Os bytes são estimativas do JSON e não incluem overhead do Engine.IO/WebSocket.",
    }


def gerar_markdown_realtime(comparacao: dict[str, Any]) -> str:
    linhas = [
        "# Comparação de tráfego em tempo real",
        "",
        "Os valores são estimativas do JSON antes do overhead do Socket.IO/Engine.IO.",
        "",
        "## Métricas gerais",
        "",
        "| Métrica | Antes | Depois | Diferença | Variação |",
        "|---|---:|---:|---:|---:|",
    ]
    for nome, item in (comparacao.get("metricas") or {}).items():
        variacao = item.get("variacao_percentual")
        variacao_txt = "n/a" if variacao is None else f"{variacao:.2f}%"
        linhas.append(f"| `{nome}` | {item['antes']:.0f} | {item['depois']:.0f} | {item['diferenca']:.0f} | {variacao_txt} |")
    linhas += ["", "## Eventos", "", "| Evento | Bytes antes | Bytes depois | Diferença | Variação |", "|---|---:|---:|---:|---:|"]
    for item in (comparacao.get("eventos") or [])[:30]:
        variacao = item.get("variacao_percentual")
        variacao_txt = "n/a" if variacao is None else f"{variacao:.2f}%"
        linhas.append(f"| `{item['evento']}` | {item['bytes_antes']:.0f} | {item['bytes_depois']:.0f} | {item['diferenca']:.0f} | {variacao_txt} |")
    return "\n".join(linhas) + "\n"
