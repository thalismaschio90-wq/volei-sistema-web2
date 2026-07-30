"""Laboratório controlado para comparar consultas/funções críticas.

O laboratório usa placeholders de ambiente para evitar versionar nomes de
competições, equipes, IDs e outros valores do banco. Ele não aplica índices nem
executa escrita. Candidatos só são aprovados quando o cenário antes/depois é
executado em homologação com dados representativos.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from core.sql_benchmark import ResultadoBenchmark, executar_cenario


class SqlLabError(RuntimeError):
    pass


@dataclass
class ComparacaoLab:
    nome: str
    baseline_media_ms: float
    candidato_media_ms: float
    baseline_p95_ms: float
    candidato_p95_ms: float
    ganho_media_percentual: float
    ganho_p95_percentual: float
    aprovado: bool
    motivo: str = ""

    def como_dict(self) -> dict[str, Any]:
        return asdict(self)


def _resolver_valor(valor: Any, ambiente: Mapping[str, str]) -> Any:
    if isinstance(valor, str) and valor.startswith("${") and valor.endswith("}"):
        nome = valor[2:-1].strip()
        if not nome:
            raise SqlLabError("Placeholder de ambiente vazio.")
        if nome not in ambiente or str(ambiente[nome]).strip() == "":
            raise SqlLabError(f"Variável obrigatória não definida: {nome}")
        bruto = ambiente[nome]
        if nome.endswith("_ID"):
            try:
                return int(bruto)
            except ValueError:
                return bruto
        return bruto
    if isinstance(valor, list):
        return [_resolver_valor(v, ambiente) for v in valor]
    if isinstance(valor, dict):
        return {k: _resolver_valor(v, ambiente) for k, v in valor.items()}
    return valor


def resolver_placeholders(cenario: Mapping[str, Any], ambiente: Mapping[str, str] | None = None) -> dict[str, Any]:
    return _resolver_valor(dict(cenario), ambiente or os.environ)



def _ganho_percentual(antes: float, depois: float) -> float:
    if antes <= 0:
        return 0.0
    return round(((antes - depois) / antes) * 100.0, 2)


def _por_nome(resultados: Sequence[ResultadoBenchmark]) -> dict[str, ResultadoBenchmark]:
    return {r.nome: r for r in resultados}


def comparar_resultados(
    baseline: Sequence[ResultadoBenchmark],
    candidato: Sequence[ResultadoBenchmark],
    *,
    ganho_minimo_percentual: float = 5.0,
    regressao_maxima_percentual: float = 5.0,
) -> list[ComparacaoLab]:
    antes = _por_nome(baseline)
    depois = _por_nome(candidato)
    comparacoes: list[ComparacaoLab] = []
    for nome in sorted(set(antes) | set(depois)):
        a = antes.get(nome)
        d = depois.get(nome)
        if not a or not d:
            comparacoes.append(ComparacaoLab(nome, 0, 0, 0, 0, 0, 0, False, "Benchmark ausente em um dos cenários."))
            continue
        if a.erro or d.erro:
            comparacoes.append(ComparacaoLab(nome, a.media_ms, d.media_ms, a.p95_ms, d.p95_ms, 0, 0, False, a.erro or d.erro))
            continue
        ganho_media = _ganho_percentual(a.media_ms, d.media_ms)
        ganho_p95 = _ganho_percentual(a.p95_ms, d.p95_ms)
        regressao = min(ganho_media, ganho_p95)
        aprovado = (ganho_media >= ganho_minimo_percentual or ganho_p95 >= ganho_minimo_percentual) and regressao >= -regressao_maxima_percentual
        motivo = "Ganho comprovado." if aprovado else "Ganho insuficiente ou regressão acima do limite."
        comparacoes.append(ComparacaoLab(
            nome=nome,
            baseline_media_ms=a.media_ms,
            candidato_media_ms=d.media_ms,
            baseline_p95_ms=a.p95_ms,
            candidato_p95_ms=d.p95_ms,
            ganho_media_percentual=round(ganho_media, 2),
            ganho_p95_percentual=round(ganho_p95, 2),
            aprovado=aprovado,
            motivo=motivo,
        ))
    return comparacoes


def executar_laboratorio(config: Mapping[str, Any], ambiente: Mapping[str, str] | None = None) -> dict[str, Any]:
    resolvido = resolver_placeholders(config, ambiente)
    baseline_cfg = resolvido.get("baseline") or {}
    candidato_cfg = resolvido.get("candidato") or {}
    if not baseline_cfg.get("benchmarks"):
        raise SqlLabError("Cenário baseline não possui benchmarks.")
    if not candidato_cfg.get("benchmarks"):
        raise SqlLabError("Cenário candidato não possui benchmarks.")

    baseline = executar_cenario(baseline_cfg)
    candidato = executar_cenario(candidato_cfg)
    comparacoes = comparar_resultados(
        baseline,
        candidato,
        ganho_minimo_percentual=float(resolvido.get("ganho_minimo_percentual", 5.0)),
        regressao_maxima_percentual=float(resolvido.get("regressao_maxima_percentual", 5.0)),
    )
    return {
        "titulo": resolvido.get("titulo") or "Laboratório SQL VolleyTablePro",
        "baseline": [r.como_dict() for r in baseline],
        "candidato": [r.como_dict() for r in candidato],
        "comparacoes": [c.como_dict() for c in comparacoes],
        "aprovado": bool(comparacoes) and all(c.aprovado for c in comparacoes),
    }


def relatorio_markdown(resultado: Mapping[str, Any]) -> str:
    linhas = [f"# {resultado.get('titulo', 'Laboratório SQL')}", "", f"**Resultado geral:** {'APROVADO' if resultado.get('aprovado') else 'NÃO APROVADO'}", "", "| Benchmark | Média antes | Média depois | Ganho média | P95 antes | P95 depois | Ganho P95 | Status |", "|---|---:|---:|---:|---:|---:|---:|---|"]
    for item in resultado.get("comparacoes") or []:
        linhas.append(
            f"| {item['nome']} | {item['baseline_media_ms']:.3f} ms | {item['candidato_media_ms']:.3f} ms | {item['ganho_media_percentual']:.2f}% | "
            f"{item['baseline_p95_ms']:.3f} ms | {item['candidato_p95_ms']:.3f} ms | {item['ganho_p95_percentual']:.2f}% | {'OK' if item['aprovado'] else 'REVISAR'} |"
        )
    linhas += ["", "## Regras", "", "- Execute baseline e candidato com os mesmos dados e no mesmo ambiente.", "- Não aplique índice em produção sem confirmar o plano e o ganho em homologação.", "- Valores de competição, equipe e partida são lidos de variáveis de ambiente e não aparecem neste arquivo."]
    return "\n".join(linhas) + "\n"


def salvar_laboratorio(resultado: Mapping[str, Any], destino_json: str | Path, destino_md: str | Path) -> None:
    Path(destino_json).write_text(json.dumps(resultado, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(destino_md).write_text(relatorio_markdown(resultado), encoding="utf-8")
