"""Regras puras da papeleta e preparação do set."""

from __future__ import annotations

from typing import Any, Iterable

POSICOES = (1, 2, 3, 4, 5, 6)


def inteiro_seguro(valor: Any, padrao: int = 0) -> int:
    try:
        return int(valor)
    except (TypeError, ValueError):
        return padrao


def set_operacional_seguro(partida: dict[str, Any]) -> int:
    """Retorna o set que realmente deve ser preenchido na papeleta."""
    partida = partida or {}
    set_banco = max(1, inteiro_seguro(partida.get("set_atual"), 1))
    esperado = max(
        1,
        inteiro_seguro(partida.get("sets_a"), 0)
        + inteiro_seguro(partida.get("sets_b"), 0)
        + 1,
    )
    sets_max = inteiro_seguro(partida.get("sets_max"), 0)
    if sets_max > 0:
        esperado = min(esperado, sets_max)
    return max(set_banco, esperado)


def papeleta_vazia() -> dict[int, str]:
    return {posicao: "" for posicao in POSICOES}


def papeleta_completa(papeleta: dict[int, Any] | None) -> bool:
    papeleta = papeleta or {}
    return all(str(papeleta.get(posicao) or "").strip() for posicao in POSICOES)


def numero_atleta(valor: Any) -> int | None:
    if valor in (None, ""):
        return None
    try:
        numero = int(valor)
    except (TypeError, ValueError):
        return None
    return numero if numero > 0 else None


def mapa_atletas_por_numero(atletas: Iterable[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    mapa: dict[int, dict[str, Any]] = {}
    for atleta in atletas or []:
        numero = numero_atleta(atleta.get("numero"))
        if numero is not None:
            mapa[numero] = atleta
    return mapa


def montar_dados_papeleta(
    atletas: Iterable[dict[str, Any]],
    valores_posicao: dict[int, Any],
) -> tuple[dict[int, dict[str, Any]], list[str]]:
    """Valida seis posições e devolve os atletas escolhidos por posição."""
    mapa = mapa_atletas_por_numero(atletas)
    dados: dict[int, dict[str, Any]] = {}
    erros: list[str] = []
    numeros_escolhidos: list[int] = []

    for posicao in POSICOES:
        bruto = str(valores_posicao.get(posicao) or "").strip()
        numero = numero_atleta(bruto)
        if numero is None:
            erros.append(f"Informe a posição {posicao}.")
            continue
        atleta = mapa.get(numero)
        if not atleta:
            erros.append(f"O número {numero} não pertence ao elenco aprovado.")
            continue
        dados[posicao] = atleta
        numeros_escolhidos.append(numero)

    repetidos = sorted({n for n in numeros_escolhidos if numeros_escolhidos.count(n) > 1})
    if repetidos:
        erros.append("Não é permitido repetir atleta na papeleta: " + ", ".join(map(str, repetidos)) + ".")

    if len(dados) != 6 and not erros:
        erros.append("Preencha as 6 posições da equipe.")
    return dados, erros


def rotacao_por_papeleta(dados: dict[int, dict[str, Any]]) -> list[str]:
    """Converte posições da papeleta para a ordem visual usada no jogo."""
    ordem = (4, 3, 2, 5, 6, 1)
    return [str((dados.get(posicao) or {}).get("numero") or "") for posicao in ordem]


def fase_exige_correcao(partida: dict[str, Any]) -> bool:
    fase = str((partida or {}).get("fase_partida") or "").strip().lower()
    status = str((partida or {}).get("status_jogo") or "").strip().lower()
    return fase in {"intervalo_set", "entre_sets"} or status == "entre_sets"


def equipes_operacionais(partida: dict[str, Any]) -> tuple[str, str]:
    partida = partida or {}
    equipe_a = str(partida.get("equipe_a_operacional") or partida.get("equipe_a") or "").strip()
    equipe_b = str(partida.get("equipe_b_operacional") or partida.get("equipe_b") or "").strip()
    return equipe_a, equipe_b
