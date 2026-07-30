"""Regras puras relacionadas a sets."""
from __future__ import annotations


def sets_para_vencer(melhor_de: int) -> int:
    melhor_de = int(melhor_de or 1)
    if melhor_de <= 1:
        return 1
    return (melhor_de // 2) + 1


def partida_terminou(sets_a: int, sets_b: int, melhor_de: int) -> bool:
    necessario = sets_para_vencer(melhor_de)
    return int(sets_a or 0) >= necessario or int(sets_b or 0) >= necessario
