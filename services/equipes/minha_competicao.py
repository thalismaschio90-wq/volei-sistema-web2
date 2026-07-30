"""Montagem dos contextos de navegação da equipe dentro de uma competição.

O módulo recebe dados já carregados e mantém fora das rotas a preparação de
partidas, rodadas, classificação e avisos. Não acessa Flask nem PostgreSQL.
"""
from __future__ import annotations

from typing import Any, Iterable


def montar_contexto_minhas_partidas(
    *,
    equipe: dict[str, Any],
    partidas: Iterable[dict[str, Any]],
    rodadas_partidas: Iterable[dict[str, Any]],
    competicao: dict[str, Any] | None,
    grupos: Iterable[dict[str, Any]],
    classificacao: dict[str, Any] | None,
    criterios_classificacao: Iterable[str],
    colunas_classificacao: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Retorna exatamente o contexto usado pela página ``minhas_partidas``."""
    return {
        "equipe": dict(equipe or {}),
        "partidas": list(partidas or []),
        "rodadas_partidas": list(rodadas_partidas or []),
        "competicao": dict(competicao or {}),
        "grupos": list(grupos or []),
        "classificacao": dict(classificacao or {}),
        "criterios_classificacao": list(criterios_classificacao or []),
        "colunas_classificacao": list(colunas_classificacao or []),
    }


def montar_contexto_minha_equipe(
    *,
    equipe: dict[str, Any],
    erro: str | None,
    sucesso: str | None,
    escudo_padrao: str,
    avisos: dict[str, Any] | None,
) -> dict[str, Any]:
    """Centraliza o contexto da tela de perfil/quadro técnico da equipe."""
    dados_avisos = dict(avisos or {})
    return {
        "equipe": dict(equipe or {}),
        "erro": erro,
        "sucesso": sucesso,
        "escudo_padrao": escudo_padrao,
        "notificacoes_equipe": list(dados_avisos.get("notificacoes_equipe") or []),
        "solicitacoes_equipe": list(dados_avisos.get("solicitacoes_equipe") or []),
        "notificacoes_nao_lidas": int(dados_avisos.get("notificacoes_nao_lidas") or 0),
    }


def resumir_documentacao_atletas(atletas: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Resumo reutilizável para conferência e documentos dos atletas."""
    lista = list(atletas or [])
    com_foto = 0
    com_instagram = 0
    com_documento = 0
    aprovados = 0
    pendentes = 0

    for atleta in lista:
        if atleta.get("foto") or atleta.get("foto_url") or atleta.get("foto_perfil"):
            com_foto += 1
        if str(atleta.get("instagram") or "").strip():
            com_instagram += 1
        if atleta.get("documento") or atleta.get("documento_url") or atleta.get("cpf"):
            com_documento += 1

        status = str(atleta.get("status") or "").strip().lower()
        if status == "aprovado":
            aprovados += 1
        elif status in {"", "pendente", "aguardando", "em análise", "em analise", "em_analise"}:
            pendentes += 1

    return {
        "total": len(lista),
        "com_foto": com_foto,
        "com_instagram": com_instagram,
        "com_documento": com_documento,
        "aprovados": aprovados,
        "pendentes": pendentes,
    }
