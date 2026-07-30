"""Coordenação do fluxo de pré-jogo do apontador.

Este módulo não conhece Flask. As rotas continuam responsáveis por sessão,
flash, redirecionamento e renderização.
"""

from __future__ import annotations

from typing import Any, Callable

from rules.pre_jogo import (
    equipe_do_lado,
    equipes_validas_partida,
    lado_valido,
    montar_fluxo_pre_jogo,
    numero_atual_normalizado,
    operador_autorizado,
    validar_numeracoes_conferencia,
)


def montar_contexto_pre_jogo(
    *,
    partida: dict[str, Any],
    arbitros: list[dict[str, Any]],
    operador_login: str,
    equipe_ja_conferida_fn: Callable[[str, str], bool],
    competicao: str,
    bloqueada_por_outro: bool,
) -> dict[str, Any]:
    equipe_a = str(partida.get("equipe_a_operacional") or "").strip()
    equipe_b = str(partida.get("equipe_b_operacional") or "").strip()
    conferida_a = bool(equipe_a and equipe_ja_conferida_fn(competicao, equipe_a))
    conferida_b = bool(equipe_b and equipe_ja_conferida_fn(competicao, equipe_b))
    fluxo = montar_fluxo_pre_jogo(partida)

    return {
        "competicao_nome": competicao,
        "partida": partida,
        "fluxo": fluxo,
        "arbitros": arbitros,
        "bloqueada_por_outro": bloqueada_por_outro,
        "equipe_a_conferida": conferida_a,
        "equipe_b_conferida": conferida_b,
        "precisa_conferencia": bool(equipe_a and equipe_b and (not conferida_a or not conferida_b)),
        "capitao_a_nome": partida.get("capitao_a_nome"),
        "capitao_a_numero": partida.get("capitao_a_numero"),
        "capitao_b_nome": partida.get("capitao_b_nome"),
        "capitao_b_numero": partida.get("capitao_b_numero"),
        "pre_jogo_bloqueado": fluxo.get("fase_partida") != "pre_jogo",
        "tie_break_pendente": bool(fluxo.get("tiebreak_pendente")),
        "operador_login_atual": operador_login,
    }


def validar_acesso_operador(partida: dict[str, Any], operador_login: str, acao: str) -> tuple[bool, str]:
    if operador_autorizado(partida, operador_login):
        return True, ""
    return False, f"Somente o operador da partida pode {acao}."


def resolver_equipe_conferencia(
    *,
    partida: dict[str, Any],
    lado: str,
    equipe_informada: str = "",
) -> tuple[bool, str, str]:
    if not lado_valido(lado):
        return False, "Lado inválido para conferência.", ""
    equipe = str(equipe_informada or "").strip() or equipe_do_lado(partida, lado)
    if not equipe:
        return False, "Equipe não definida para conferência.", ""
    if equipe not in equipes_validas_partida(partida):
        return False, "Equipe inválida para conferência.", ""
    return True, "", equipe


def preparar_alteracoes_numeracao(
    *,
    atletas: list[dict[str, Any]],
    ids: list[str],
    valores: dict[str, Any],
) -> tuple[list[tuple[str, int | None]], list[str]]:
    atletas_por_id = {str(a.get("id")): a for a in atletas}
    novos_numeros, erros = validar_numeracoes_conferencia(atletas_por_id, ids, valores)
    if erros:
        return [], erros

    alteracoes: list[tuple[str, int | None]] = []
    for atleta_id, numero in novos_numeros.items():
        atual = numero_atual_normalizado((atletas_por_id.get(atleta_id) or {}).get("numero"))
        if atual != numero:
            alteracoes.append((atleta_id, numero))
    return alteracoes, []


def contexto_capitao(
    *,
    partida: dict[str, Any],
    lado: str,
    atletas: list[dict[str, Any]],
    competicao: str,
) -> dict[str, Any]:
    equipe = equipe_do_lado(partida, lado)
    atletas_numerados = [a for a in atletas if a.get("numero") not in (None, "")]
    atleta_atual_id = partida.get("capitao_a_id") if str(lado).upper() == "A" else partida.get("capitao_b_id")
    return {
        "competicao_nome": competicao,
        "partida": partida,
        "lado": str(lado).upper(),
        "equipe_nome": equipe,
        "atletas": atletas_numerados,
        "atleta_atual_id": atleta_atual_id,
    }
