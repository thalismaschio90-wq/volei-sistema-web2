"""Coordenação da papeleta e preparação do set.

Não conhece Flask e não emite Socket.IO. As rotas seguem responsáveis por
sessão, mensagens, redirecionamento e publicação em tempo real.
"""

from __future__ import annotations

from typing import Any, Callable

from rules.papeleta import (
    POSICOES,
    equipes_operacionais,
    montar_dados_papeleta,
    papeleta_completa,
    papeleta_vazia,
    rotacao_por_papeleta,
    set_operacional_seguro,
)


def valores_formulario(form: Any, lado: str) -> dict[int, Any]:
    return {posicao: form.get(f"{lado}_{posicao}") for posicao in POSICOES}


def preparar_escalacao(
    *,
    atletas: list[dict[str, Any]],
    valores: dict[int, Any],
) -> tuple[dict[int, dict[str, Any]], list[str], list[str]]:
    dados, erros = montar_dados_papeleta(atletas, valores)
    rotacao = rotacao_por_papeleta(dados) if not erros else []
    return dados, rotacao, erros


def montar_contexto_papeleta(
    *,
    competicao: str,
    partida: dict[str, Any],
    equipe_a: str,
    equipe_b: str,
    set_atual: int,
    atletas_a: list[dict[str, Any]],
    atletas_b: list[dict[str, Any]],
    papeleta_a: dict[int, Any] | None,
    papeleta_b: dict[int, Any] | None,
) -> dict[str, Any]:
    papeleta_a = papeleta_a or papeleta_vazia()
    papeleta_b = papeleta_b or papeleta_vazia()
    return {
        "competicao_nome": competicao,
        "partida": partida,
        "equipe_a": equipe_a,
        "equipe_b": equipe_b,
        "atletas_a": [a for a in atletas_a if a.get("numero") not in (None, "")],
        "atletas_b": [a for a in atletas_b if a.get("numero") not in (None, "")],
        "papeleta_a": papeleta_a,
        "papeleta_b": papeleta_b,
        "fluxo": {
            "fase_partida": str(partida.get("fase_partida") or "papeleta").strip().lower(),
            "papeleta_a_completa": papeleta_completa(papeleta_a),
            "papeleta_b_completa": papeleta_completa(papeleta_b),
            "set_atual": set_atual,
        },
    }


def papeletas_set_completas(
    *,
    partida_id: int,
    competicao: str,
    partida: dict[str, Any],
    verificar_fn: Callable[[int, str, str, int], bool],
) -> bool:
    set_atual = set_operacional_seguro(partida)
    equipe_a, equipe_b = equipes_operacionais(partida)
    if not equipe_a or not equipe_b:
        return False
    try:
        ok_a = bool(verificar_fn(partida_id, competicao, equipe_a, set_atual))
    except Exception:
        ok_a = False
    try:
        ok_b = bool(verificar_fn(partida_id, competicao, equipe_b, set_atual))
    except Exception:
        ok_b = False
    return ok_a and ok_b


def montar_estado_inicial_jogo(
    *,
    competicao: str,
    partida_id: int,
    partida: dict[str, Any],
    equipe_a: str,
    equipe_b: str,
    set_atual: int,
    rotacao_a: list[str],
    rotacao_b: list[str],
) -> dict[str, Any]:
    return {
        "ok": True,
        "competicao": competicao,
        "partida_id": partida_id,
        "equipe_a": equipe_a or "",
        "equipe_b": equipe_b or "",
        "equipe_a_operacional": equipe_a or "",
        "equipe_b_operacional": equipe_b or "",
        "equipe_a_cadastro": partida.get("equipe_a") or equipe_a or "",
        "equipe_b_cadastro": partida.get("equipe_b") or equipe_b or "",
        "pontos_a": int(partida.get("pontos_a") or 0),
        "pontos_b": int(partida.get("pontos_b") or 0),
        "placar_a": int(partida.get("pontos_a") or 0),
        "placar_b": int(partida.get("pontos_b") or 0),
        "sets_a": int(partida.get("sets_a") or 0),
        "sets_b": int(partida.get("sets_b") or 0),
        "set_atual": int(set_atual or 1),
        "saque_atual": partida.get("saque_atual") or partida.get("saque_inicial") or "",
        "rotacao_a": list(rotacao_a),
        "rotacao_b": list(rotacao_b),
        "historico": [{"descricao": "Jogo iniciado"}],
        "ultima_acao": "Jogo iniciado",
        "fase_partida": "jogo",
        "status_jogo": "em_andamento",
    }
