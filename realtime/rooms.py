"""Nomes de salas usados pela comunicação em tempo real.

Este módulo não conhece Flask-SocketIO. Ele apenas produz nomes estáveis,
permitindo que publicação e inscrição usem exatamente a mesma convenção.
"""
from __future__ import annotations


def normalizar_id_partida(partida_id: object) -> str:
    return str(partida_id or "").strip()


def sala_arbitros(partida_id: object) -> str:
    return normalizar_id_partida(partida_id)


def salas_partida(partida_id: object, competicao: object = None) -> list[str]:
    base = normalizar_id_partida(partida_id)
    comp = str(competicao or "").strip()
    if not base:
        return []

    salas = [
        base,
        f"partida:{base}",
        f"partida_{base}",
        f"arbitros:{base}",
        f"arbitros_{base}",
    ]
    if comp:
        salas.extend(
            [
                f"partida:{comp}:{base}",
                f"partida_{comp}_{base}",
                f"arbitros:{comp}:{base}",
                f"arbitros_{comp}_{base}",
            ]
        )
    return list(dict.fromkeys(sala for sala in salas if sala))


def sala_placar_apontador(apontador: object) -> str:
    login = str(apontador or "").strip()
    return f"placar_apontador:{login}" if login else ""


def sala_delta(partida_id: object) -> str:
    base = normalizar_id_partida(partida_id)
    return f"delta:{base}" if base else ""


def sala_legacy(partida_id: object) -> str:
    base = normalizar_id_partida(partida_id)
    return f"legacy:{base}" if base else ""
