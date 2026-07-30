"""Regras puras do fluxo de pré-jogo do apontador."""

from __future__ import annotations

from typing import Any, Iterable

LADOS_VALIDOS = {"A", "B"}


def normalizar_lado(lado: Any) -> str:
    return str(lado or "").strip().upper()


def lado_valido(lado: Any) -> bool:
    return normalizar_lado(lado) in LADOS_VALIDOS


def equipe_do_lado(partida: dict[str, Any], lado: Any, *, preferir_operacional: bool = True) -> str:
    lado_norm = normalizar_lado(lado)
    if lado_norm not in LADOS_VALIDOS:
        return ""
    sufixo = "a" if lado_norm == "A" else "b"
    if preferir_operacional:
        equipe = str(partida.get(f"equipe_{sufixo}_operacional") or "").strip()
        if equipe:
            return equipe
    return str(partida.get(f"equipe_{sufixo}") or "").strip()


def operador_autorizado(partida: dict[str, Any], operador_login: Any) -> bool:
    operador = str(operador_login or "").strip()
    return bool(operador) and str(partida.get("operador_login") or "").strip() == operador


def fase_fluxo_pre_jogo(partida: dict[str, Any]) -> str:
    fase = str(partida.get("fase_partida") or partida.get("status_jogo") or "pre_jogo").strip().lower()
    if fase in {"", "aguardando", "agendada", "agendado", "reservado", "livre"}:
        return "pre_jogo"
    return fase


def montar_fluxo_pre_jogo(partida: dict[str, Any]) -> dict[str, Any]:
    return {
        "fase_partida": fase_fluxo_pre_jogo(partida),
        "tiebreak_pendente": bool(partida.get("tiebreak_pendente")),
    }


def equipes_validas_partida(partida: dict[str, Any]) -> set[str]:
    return {
        str(partida.get("equipe_a") or "").strip(),
        str(partida.get("equipe_b") or "").strip(),
    } - {""}


def validar_numeracoes_conferencia(
    atletas_por_id: dict[str, dict[str, Any]],
    ids: Iterable[Any],
    valores: dict[str, Any],
) -> tuple[dict[str, int | None], list[str]]:
    novos_numeros: dict[str, int | None] = {}
    numeros_usados: dict[int, list[str]] = {}
    erros: list[str] = []

    for atleta_id_bruto in ids:
        atleta_id = str(atleta_id_bruto).strip()
        atleta = atletas_por_id.get(atleta_id)
        if not atleta:
            continue

        bruto = str(valores.get(atleta_id, "") or "").strip()
        if bruto == "":
            novos_numeros[atleta_id] = None
            continue

        try:
            numero = int(bruto)
        except (TypeError, ValueError):
            erros.append(f"Número inválido para {atleta.get('nome') or 'atleta'}.")
            continue

        if numero < 1 or numero > 99:
            erros.append(f"O número de {atleta.get('nome') or 'atleta'} precisa ser entre 1 e 99.")
            continue

        novos_numeros[atleta_id] = numero
        numeros_usados.setdefault(numero, []).append(atleta.get("nome") or f"Atleta {atleta_id}")

    for numero, nomes in numeros_usados.items():
        if len(nomes) > 1:
            erros.append(f"O número {numero} foi informado para mais de uma atleta: {', '.join(nomes)}.")

    return novos_numeros, erros


def numero_atual_normalizado(valor: Any) -> int | None:
    if valor in (None, ""):
        return None
    try:
        return int(valor)
    except (TypeError, ValueError):
        return None
