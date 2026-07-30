"""Serviço do painel inicial do Super ADM."""
from __future__ import annotations

from banco import conectar
from repositories.superadmin_painel import buscar_painel_superadmin


def montar_painel_superadmin(login: str) -> dict:
    return buscar_painel_superadmin(login, conectar_fn=conectar)
