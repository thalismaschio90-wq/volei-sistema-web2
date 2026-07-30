"""Exportação segura do diagnóstico de performance.

Não inclui SQL bruto, parâmetros, query strings nem dados pessoais. O relatório usa
somente métricas agregadas e a estrutura anonimizada já mantida pelo performance_store.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any


def _identificador_seguro(valor: str) -> str:
    texto = re.sub(r"[^a-zA-Z0-9_]+", "_", str(valor or "").strip())
    texto = texto.strip("_")[:63]
    return texto or "campo"


def _candidato_indice(consulta: dict[str, Any]) -> str | None:
    estrutura = consulta.get("estrutura") or {}
    tabelas = estrutura.get("tabelas") or []
    filtros = estrutura.get("filtros") or []
    ordenacao = estrutura.get("ordenacao") or []
    if not tabelas:
        return None

    tabela = _identificador_seguro(str(tabelas[0]).split(".")[-1])
    colunas: list[str] = []
    for valor in [*filtros, *ordenacao]:
        coluna = _identificador_seguro(str(valor).split(".")[-1])
        if coluna and coluna not in colunas:
            colunas.append(coluna)
    if not colunas:
        return None

    colunas = colunas[:5]
    nome = _identificador_seguro("idx_vtp_" + tabela + "_" + "_".join(colunas))
    return f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {nome} ON {tabela} ({', '.join(colunas)});"


def montar_exportacao(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Cria estrutura estável para JSON e Markdown."""
    consultas = []
    for posicao, item in enumerate(snapshot.get("consultas_lentas") or [], start=1):
        registro = dict(item)
        registro["prioridade"] = posicao
        registro["indice_candidato"] = _candidato_indice(item)
        registro["validacao_recomendada"] = (
            "Executar EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) na consulta original "
            "em homologação e comparar antes/depois."
        )
        consultas.append(registro)

    return {
        "titulo": "Diagnóstico de performance — VolleyTablePro",
        "gerado_em_iso": datetime.now(timezone.utc).isoformat(),
        "backend": snapshot.get("backend"),
        "iniciado_em": snapshot.get("iniciado_em"),
        "limites": snapshot.get("limites") or {},
        "resumo": {
            "rotas_observadas": len(snapshot.get("rotas") or []),
            "consultas_lentas_distintas": len(consultas),
        },
        "rotas_prioritarias": list(snapshot.get("rotas") or []),
        "consultas_prioritarias": consultas,
        "observacoes": [
            "O relatório não contém SQL bruto, parâmetros, CPF, e-mail, senha ou conteúdo de formulários.",
            "Índices são apenas candidatos estruturais e não devem ser aplicados sem EXPLAIN ANALYZE em homologação.",
            "CREATE INDEX CONCURRENTLY não pode ser executado dentro de uma transação explícita.",
        ],
    }


def exportar_json(snapshot: dict[str, Any]) -> str:
    return json.dumps(montar_exportacao(snapshot), ensure_ascii=False, indent=2)


def exportar_markdown(snapshot: dict[str, Any]) -> str:
    dados = montar_exportacao(snapshot)
    linhas = [
        f"# {dados['titulo']}",
        "",
        f"Gerado em: `{dados['gerado_em_iso']}`",
        "",
        "## Resumo",
        "",
        f"- Rotas observadas: **{dados['resumo']['rotas_observadas']}**",
        f"- Consultas lentas distintas: **{dados['resumo']['consultas_lentas_distintas']}**",
        f"- Backend de métricas: **{dados.get('backend') or 'não informado'}**",
        "",
        "## Rotas prioritárias",
        "",
        "| # | Método | Endpoint | Chamadas | P95 | Média | SQL médio | Consultas médias | 5xx |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for i, rota in enumerate(dados["rotas_prioritarias"][:100], start=1):
        endpoint = str(rota.get("endpoint") or rota.get("rota") or "-").replace("|", "\\|")
        linhas.append(
            f"| {i} | {rota.get('metodo','')} | `{endpoint}` | {rota.get('quantidade',0)} | "
            f"{rota.get('duracao_p95_ms',0):.1f} ms | {rota.get('duracao_media_ms',0):.1f} ms | "
            f"{rota.get('sql_media_ms',0):.1f} ms | {rota.get('sql_media_consultas',0):.1f} | {rota.get('erros',0)} |"
        )

    linhas.extend(["", "## Consultas lentas prioritárias", ""])
    if not dados["consultas_prioritarias"]:
        linhas.append("Nenhuma consulta ultrapassou o limite configurado.")
    for item in dados["consultas_prioritarias"][:100]:
        estrutura = item.get("estrutura") or {}
        linhas.extend([
            f"### {item['prioridade']}. `{item.get('fingerprint','')}` — {item.get('operacao','SQL')}",
            "",
            f"- Ocorrências: **{item.get('quantidade',0)}**",
            f"- Média: **{item.get('duracao_media_ms',0):.1f} ms**",
            f"- Máxima: **{item.get('duracao_max_ms',0):.1f} ms**",
            f"- Origem: {', '.join('`'+x+'`' for x in item.get('origens') or []) or 'não identificada'}",
            f"- Rotas: {', '.join('`'+x+'`' for x in item.get('rotas') or []) or 'não identificadas'}",
            f"- Tabelas: {', '.join('`'+x+'`' for x in estrutura.get('tabelas') or []) or 'não identificadas'}",
            f"- Filtros: {', '.join('`'+x+'`' for x in estrutura.get('filtros') or []) or 'não identificados'}",
            f"- Ordenação: {', '.join('`'+x+'`' for x in estrutura.get('ordenacao') or []) or 'não identificada'}",
            "",
        ])
        for sugestao in item.get("sugestoes") or []:
            linhas.append(f"- **{sugestao.get('titulo','Revisão')}**: {sugestao.get('detalhe','')}")
        candidato = item.get("indice_candidato")
        if candidato:
            linhas.extend([
                "",
                "Candidato para homologação — **não aplicar diretamente em produção**:",
                "",
                "```sql",
                candidato,
                "```",
            ])
        linhas.extend([
            "",
            "Validação recomendada:",
            "",
            "```sql",
            "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)",
            "-- cole aqui a consulta original usando parâmetros de homologação",
            ";",
            "```",
            "",
        ])

    linhas.extend(["## Observações de segurança", ""])
    linhas.extend(f"- {texto}" for texto in dados["observacoes"])
    linhas.append("")
    return "\n".join(linhas)
