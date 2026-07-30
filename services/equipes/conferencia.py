"""Regras e montagem da conferência geral de atletas."""
from __future__ import annotations

from collections import OrderedDict
from typing import Any, Iterable

from repositories.conferencia_atletas import (
    buscar_configuracao,
    definir_status,
    listar_atletas,
    salvar_configuracao,
)
from services.equipes.minha_competicao import resumir_documentacao_atletas


def _texto(valor: Any) -> str:
    return str(valor or "").strip()


def agrupar_atletas_por_equipe(atletas: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grupos: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
    for atleta in atletas or []:
        item = dict(atleta or {})
        equipe = _texto(item.get("equipe")) or "Sem equipe"
        grupos.setdefault(equipe, []).append(item)
    return dict(grupos)


def montar_contexto_conferencia(nome_competicao: str) -> dict[str, Any]:
    configuracao = buscar_configuracao(nome_competicao)
    atletas = listar_atletas(nome_competicao) if configuracao else []
    return {
        "configuracao": configuracao,
        "equipes": agrupar_atletas_por_equipe(atletas),
        "resumo_documentacao": resumir_documentacao_atletas(atletas),
    }


def atualizar_configuracao(
    nome_competicao: str,
    *,
    prazo: Any,
    link: Any,
    aprovacao_automatica: bool,
) -> bool:
    return salvar_configuracao(
        nome_competicao,
        prazo=_texto(prazo),
        link=_texto(link),
        aprovacao_automatica=bool(aprovacao_automatica),
    )


def liberar(nome_competicao: str) -> bool:
    return definir_status(nome_competicao, liberada=True, encerrada=False)


def encerrar(nome_competicao: str) -> bool:
    return definir_status(nome_competicao, encerrada=True)
