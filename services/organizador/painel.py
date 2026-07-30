"""Serviço do painel inicial do organizador."""
from __future__ import annotations

from banco import conectar
from repositories.organizador_painel import buscar_painel_organizador


def montar_painel_organizador(login: str, competicao_preferida: str = "") -> dict:
    dados = buscar_painel_organizador(
        login,
        competicao_preferida,
        conectar_fn=conectar,
    )
    nomes = dados.get("nomes_competicoes") or []
    atual = dados.get("competicao_atual") or ""
    tem_competicao = bool(nomes or atual)
    return {
        "competicoes": dados.get("competicoes") or [],
        "competicao_atual": atual,
        "competicao_vinculada": atual,
        "competicao": atual,
        "tem_competicao": tem_competicao,
        "total_competicoes": len(nomes),
        "operacao_liberada": tem_competicao,
        "mensagem": None if tem_competicao else "Você ainda não possui competição cadastrada.",
        "status_config": dados.get("status_config") or {},
        "solicitacoes_pendentes": int(dados.get("solicitacoes_pendentes") or 0),
        "ultimas_solicitacoes": dados.get("ultimas_solicitacoes") or [],
        "notificacoes_organizador": dados.get("notificacoes_organizador") or [],
    }
