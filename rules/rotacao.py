"""Regras puras de rotação e saque do jogo de voleibol.

A ordem interna/visual mantida pelo sistema é [IV, III, II, V, VI, I].
Este módulo não acessa banco, cache, Flask ou Socket.IO.
"""
from __future__ import annotations

import json
from typing import Any, Iterable, Mapping

ROTACAO_VAZIA = ("", "", "", "", "", "")


def normalizar_rotacao(rotacao: Any) -> list[str]:
    """Converte formatos legados para uma lista estável de seis camisas."""
    if isinstance(rotacao, str):
        try:
            rotacao = json.loads(rotacao or "[]")
        except Exception:
            rotacao = []
    if isinstance(rotacao, tuple):
        rotacao = list(rotacao)
    if not isinstance(rotacao, list):
        rotacao = []

    resultado: list[str] = []
    for item in rotacao[:6]:
        if isinstance(item, Mapping):
            numero = (
                item.get("numero")
                or item.get("camisa")
                or item.get("numero_camisa")
                or item.get("n")
                or ""
            )
        else:
            numero = item
        resultado.append(str(numero or "").strip())

    while len(resultado) < 6:
        resultado.append("")
    return resultado[:6]


def tem_seis_atletas_validos(rotacao: Any) -> bool:
    normalizada = normalizar_rotacao(rotacao)
    preenchidos = [numero for numero in normalizada if numero]
    return len(preenchidos) == 6 and len(set(preenchidos)) == 6


def rotacao_valida_ou_vazia(rotacao: Any) -> list[str]:
    normalizada = normalizar_rotacao(rotacao)
    return normalizada if tem_seis_atletas_validos(normalizada) else list(ROTACAO_VAZIA)


def girar_rotacao(rotacao: Any) -> list[str]:
    """Aplica um giro oficial na ordem visual [IV, III, II, V, VI, I]."""
    atual = normalizar_rotacao(rotacao)
    if not tem_seis_atletas_validos(atual):
        return atual
    return [
        atual[3],  # novo IV  = antigo V
        atual[0],  # novo III = antigo IV
        atual[1],  # novo II  = antigo III
        atual[4],  # novo V   = antigo VI
        atual[5],  # novo VI  = antigo I
        atual[2],  # novo I   = antigo II
    ]


def validar_rotacao(rotacao: Any, atletas_validos: Iterable[Any] | None = None) -> dict[str, Any]:
    normalizada = normalizar_rotacao(rotacao)
    erros: list[str] = []
    preenchidos = [numero for numero in normalizada if numero]

    if len(preenchidos) != 6:
        erros.append("A rotação precisa ter 6 atletas.")

    repetidos = sorted({numero for numero in preenchidos if preenchidos.count(numero) > 1})
    if repetidos:
        erros.append("Repetidos: " + ", ".join(repetidos))

    if atletas_validos is not None:
        validos = {str(numero).strip() for numero in atletas_validos}
        invalidos = [numero for numero in preenchidos if numero not in validos]
        if invalidos:
            erros.append("Inválidos: " + ", ".join(invalidos))

    return {"ok": not erros, "erros": erros, "rotacao": normalizada}


def normalizar_lado_saque(valor: Any, partida: Mapping[str, Any] | None = None) -> str:
    """Resolve saque armazenado como lado A/B ou como nome da equipe."""
    texto = str(valor or "").strip()
    if not texto:
        return ""
    lado = texto.upper()
    if lado in {"A", "B"}:
        return lado

    partida = partida or {}
    equipe_a = str(partida.get("equipe_a_operacional") or partida.get("equipe_a") or "").strip().casefold()
    equipe_b = str(partida.get("equipe_b_operacional") or partida.get("equipe_b") or "").strip().casefold()
    if texto.casefold() == equipe_a:
        return "A"
    if texto.casefold() == equipe_b:
        return "B"
    return ""


def aplicar_recuperacao_saque(
    *,
    rotacao_a: Any,
    rotacao_b: Any,
    saque_antes: Any,
    equipe_pontuadora: Any,
) -> dict[str, Any]:
    """Calcula de forma atômica saque e rotações após um ponto.

    A equipe gira somente quando conquista o saque. Nenhuma persistência ou
    publicação ocorre aqui; o chamador recebe um estado completo e consistente.
    """
    equipe = str(equipe_pontuadora or "").strip().upper()
    if equipe not in {"A", "B"}:
        raise ValueError("Equipe pontuadora inválida.")

    a = rotacao_valida_ou_vazia(rotacao_a)
    b = rotacao_valida_ou_vazia(rotacao_b)
    saque = str(saque_antes or "").strip().upper()
    if saque not in {"A", "B"}:
        saque = equipe

    a_antes = list(a)
    b_antes = list(b)
    girou = saque != equipe
    equipe_girou = equipe if girou else ""

    if girou:
        if equipe == "A":
            a = girar_rotacao(a)
        else:
            b = girar_rotacao(b)

    return {
        "rotacao_a": a,
        "rotacao_b": b,
        "rotacao_a_antes": a_antes,
        "rotacao_b_antes": b_antes,
        "saque_antes": saque,
        "saque_atual": equipe,
        "saque_depois": equipe,
        "girou": girou,
        "equipe_girou": equipe_girou,
    }


def substituir_atleta(rotacao: Any, numero_sai: Any, numero_entra: Any) -> list[str]:
    """Substitui uma camisa sem reordenar as demais posições."""
    atual = normalizar_rotacao(rotacao)
    sai = str(numero_sai or "").strip()
    entra = str(numero_entra or "").strip()
    if not sai or not entra:
        return atual
    return [entra if numero == sai else numero for numero in atual]
