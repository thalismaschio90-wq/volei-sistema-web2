"""Regras de apresentação do painel inicial da equipe.

Este módulo não acessa Flask nem banco. Ele recebe dados já carregados e devolve
um resumo pequeno, fácil de testar e reutilizar.
"""
from __future__ import annotations

from typing import Any, Iterable


_STATUS_PENDENTES = {"", "pendente", "aguardando", "em análise", "em analise", "em_analise"}


def _status(atleta: dict[str, Any]) -> str:
    return str(atleta.get("status") or "").strip().lower()


def resumir_atletas(atletas: Iterable[dict[str, Any]], limite_atletas: Any = 12) -> dict[str, Any]:
    lista = list(atletas or [])
    try:
        limite = max(0, int(limite_atletas or 12))
    except (TypeError, ValueError):
        limite = 12

    aprovados = sum(1 for atleta in lista if _status(atleta) == "aprovado")
    pendentes = sum(1 for atleta in lista if _status(atleta) in _STATUS_PENDENTES)
    reprovados = sum(1 for atleta in lista if _status(atleta) == "reprovado")
    total = len(lista)
    percentual = min(100, round((total / limite) * 100)) if limite > 0 else 0

    if limite > 0 and total >= limite and pendentes == 0 and reprovados == 0:
        status_equipe = "Equipe completa"
        status_classe = "tag-aprovado"
    elif pendentes > 0:
        status_equipe = "Aguardando conferência"
        status_classe = "tag-pendente"
    elif reprovados > 0:
        status_equipe = "Possui atleta reprovado"
        status_classe = "tag-reprovado"
    else:
        status_equipe = "Equipe em andamento"
        status_classe = "tag-info"

    return {
        "total_atletas": total,
        "limite_atletas": limite,
        "percentual_atletas": percentual,
        "atletas_aprovados": aprovados,
        "atletas_pendentes": pendentes,
        "atletas_reprovados": reprovados,
        "status_equipe": status_equipe,
        "status_classe": status_classe,
    }



def resumir_atletas_por_contadores(contadores: dict[str, Any] | None, limite_atletas: Any = 12) -> dict[str, Any]:
    """Monta o mesmo resumo visual a partir de contadores já agregados no banco."""
    dados = dict(contadores or {})
    try:
        limite = max(0, int(limite_atletas or 12))
    except (TypeError, ValueError):
        limite = 12

    total = max(0, int(dados.get("total") or 0))
    aprovados = max(0, int(dados.get("aprovados") or 0))
    pendentes = max(0, int(dados.get("pendentes") or 0))
    reprovados = max(0, int(dados.get("reprovados") or 0))
    percentual = min(100, round((total / limite) * 100)) if limite > 0 else 0

    if limite > 0 and total >= limite and pendentes == 0 and reprovados == 0:
        status_equipe = "Equipe completa"
        status_classe = "tag-aprovado"
    elif pendentes > 0:
        status_equipe = "Aguardando conferência"
        status_classe = "tag-pendente"
    elif reprovados > 0:
        status_equipe = "Possui atleta reprovado"
        status_classe = "tag-reprovado"
    else:
        status_equipe = "Equipe em andamento"
        status_classe = "tag-info"

    return {
        "total_atletas": total,
        "limite_atletas": limite,
        "percentual_atletas": percentual,
        "atletas_aprovados": aprovados,
        "atletas_pendentes": pendentes,
        "atletas_reprovados": reprovados,
        "status_equipe": status_equipe,
        "status_classe": status_classe,
    }

def proxima_partida(partidas: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    for partida in partidas or []:
        if partida.get("minha_partida") and not partida.get("finalizada"):
            return partida
    return None


def montar_resumo_painel(
    atletas: Iterable[dict[str, Any]] | None,
    partidas: Iterable[dict[str, Any]],
    controle_inscricao: dict[str, Any] | None,
) -> dict[str, Any]:
    controle = dict(controle_inscricao or {})
    if isinstance(atletas, dict) and "total" in atletas:
        resumo = resumir_atletas_por_contadores(atletas, controle.get("limite_atletas", 12))
    else:
        resumo = resumir_atletas(atletas or [], controle.get("limite_atletas", 12))
    lista_partidas = list(partidas or [])
    resumo.update(
        {
            "minhas_partidas": [p for p in lista_partidas if p.get("minha_partida")],
            "proxima_partida": proxima_partida(lista_partidas),
        }
    )
    return resumo
