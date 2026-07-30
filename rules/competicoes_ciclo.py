"""Regras puras do ciclo de vida das competições."""
from __future__ import annotations

import re


def normalizar_nome_competicao(nome: object) -> str:
    return " ".join(str(nome or "").strip().split())


def normalizar_motivo_travamento(motivo: object) -> str:
    valor = " ".join(str(motivo or "").strip().split())
    return valor or "primeiro_ponto"


def slug_login_organizador(nome: object) -> str:
    base = normalizar_nome_competicao(nome).lower()
    base = re.sub(r"[^a-z0-9]+", ".", base).strip(".")
    return base or "organizador"
